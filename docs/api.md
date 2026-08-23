# API 参考

对着实现写的，不是对着计划写的。每个签名都以 `python/tinyray/` 里的为准。

---

## 模块

| 名字 | 是什么 |
|---|---|
| `tinyray.join(...)` | 报到，返回 `Member` |
| `tinyray.pool(name)` | 拿一个 `Pool` |
| `tinyray.apool(name)` | 拿一个 `AsyncPool`（方法返回 awaitable） |
| `tinyray.__version__` | 装上的版本号 |
| `tinyray.MAX_STATE` | state 的硬上限，16 KB |
| `tinyray.FIRST_BEAT_S` | `join(timeout=)` 的默认值，30 秒 |

---

## `join()`

```python
tinyray.join(
    pool: str,
    policy: str = "churn",
    *,
    slot: int | None = None,
    size: int | None = None,
    url: str | None = None,
    serves: Any = None,
    exclusive: bool = False,
    max_concurrency: int | None = None,
    timeout: float = FIRST_BEAT_S,
) -> Member
```

阻塞到第一拍落地。联系不上抛 `Unreachable`，座位被更晚的任期占着抛 `SeatTaken`，
池子形状对不上抛 `PolicyError`。**一个进程只能加入一个 pool。**

### policy

| policy | 有座位号 | 用在哪 |
|---|---|---|
| `churn` | 否 | 可互换的一群，随时进出 |
| `serving` | 否 | 可互换，但对外提供服务 |
| `stateful` | **是** | 分片持有者，座位不可互换 |
| `collective` | **是** | 要一起点名的一组（`size=` 必填） |

有座位的策略下 `slot=` 必填，或者从 `TINYRAY_SLOT` / `RANK` / `SLURM_PROCID` /
`OMPI_COMM_WORLD_RANK` 里读；`size=` 同理，来自 `TINYRAY_SIZE` / `WORLD_SIZE` 等。

### 其余参数

- **`serves=`** —— 交一个对象，它的公开方法（不以 `_` 开头）成为接口，
  类型标注即校验表。地址自动登记。
- **`exclusive=True`** —— 座位有人就拒绝，抛 `SeatTaken`。选主要的是这个；
  默认相反，因为重启的 rank 必须能在旧租约还没过期时拿回座位。
- **`max_concurrency=`** —— 同时执行的调用数上限。超了返回 `NotDelivered`
  给调用方，不排队。默认无限制。
- **`url=`** —— 手工指定对外地址。默认由路由表探出来；多网卡机器上可以用
  `TINYRAY_ADVERTISE` 指定。

---

## `Member`

这个进程自己的注册。

### 属性

| 属性 | 说明 |
|---|---|
| `identity` | `"pool/座位#任期"`，和别人手里 Handle 上的那串一致 |
| `pool` / `slot` / `incarnation` | 分解开的同一件事 |
| `state` | 当前发布出去的 state（副本） |
| `accepted` | `False` 表示座位已被更晚的任期拿走 |
| `silence_ms` | 距离上一次成功心跳多久。它涨的时候一切照常，只是失效检测变慢 |
| `last_error` | 最近一次心跳失败的原因，恢复后仍保留 |

### 发布状态

```python
me.ready(**state) -> Member        # 合并进已有 state，并标记 ready
me.set_ready(state: dict) -> Member  # 整体替换
me.unready() -> Member             # 保留 state，但标记为不可用
me.flush(timeout=10.0) -> Member   # 阻塞到注册中心确实收下
```

`ready()` 是**合并**，所以发出去的 key 拿不回来 —— 要清掉就用 `set_ready()`。

`flush()` 最多多等一拍：调用时若有心跳在途，那一拍是改之前组装的，确认要等到
再下一拍。联系不上会抛 `TimeoutError`，座位被抢会抛 `SeatTaken`。

!!! note "state 是硬上限"
    16 KB，超了直接报错。它和 RPC 的体积限制不一样：state 会复制给**每一个
    订阅者**，实测 6 MB 到 20 个订阅者变成 120 MB。这条限制保护的是别人。

### 知道自己被顶替

```python
me.wait_fenced(timeout: float | None = None) -> bool
await me.await_fenced(timeout=None) -> bool
```

阻塞到有更晚的任期拿走这个座位，返回 `True`；超时返回 `False`。事件驱动，不轮询。

**需要联系得上注册中心。** 网络分区时它会一直等 —— 联系不上就无从得知自己被
换了。所以它防的是"被替换"，不是"脑裂"。

### 离开

```python
me.leave() -> None
with tinyray.join(...) as me: ...   # 等价
```

正常退出会自动调用。座位立刻空出来，不必等租约。

---

## `Pool` / `AsyncPool`

```python
pool = tinyray.pool("engine")
apool = tinyray.apool("engine")     # 同样的查询，方法返回 awaitable
```

**所有查询读本地缓存，不走网络** —— 没有超时，也不给注册中心加压。缓存落后真相
约一个 RTT（默认配置实测 21 毫秒），因为注册中心是在变化发生时把答复送回来的，
而不是等你下一拍来问。第一次查某个 pool 会等第一次答复到达（构造 `Pool` 就是订阅，
所以在启动时把要用的 pool 都建出来，这一次等待就没了）。

### 查询

```python
pool.all(**filt) -> list[Handle]              # 只给 ready 的
pool.pick(**filt) -> Handle                   # ready 的里面随机一个，没有则 NotFound
pool.slot(k, require_ready=False) -> Handle   # 按座位号，空座 NotFound
pool.wait(count=1, timeout=30.0, **filt) -> list[Handle]
len(pool)                                      # ready 的人数
```

`**filt` 按 state 的键值**相等**匹配。数字按值比（`shard=6/2` 找得到发布
`shard=3` 的人），布尔严格（`free=1` 不匹配 `free=True`）。

### 快照与变化

```python
pool.snapshot(include_unready=True) -> Snapshot
pool.changes(since=None, timeout=None)          # 生成器，产出 Snapshot
apool.achanges(since=None, timeout=None)        # 异步生成器
```

`changes()` 阻塞在事件上，**不轮询**。池子不动就不返回。成员被顶替后流会结束，
不会让消费者永远挂着。

!!! info "为什么是快照流，不是事件流"
    客户端以心跳为采样率，注册中心会把一个间隔内的多次变化折叠成"该成员的当前
    状态"。所以承诺"不丢事件"是协议兑现不了的；快照能诚实兑现"不丢状态"。
    事件是一次 diff 的事 —— 每条记录都带 `incarnation` 和 `ready`。

### 点名

```python
pool.epoch(min=None, timeout=60.0) -> Epoch
```

等到人齐（默认按池子的 `size`，或 `min=`）然后冻住。**只有当指纹恰好是它自己
那份名单算出来的时候，这一轮才会被交出去** —— 所以各 rank 指纹相同就意味着名单
相同。联系不上注册中心会抛 `Stale`，宁可不开也不开一轮不可信的。

---

## `Snapshot`

某个 revision 上的一份定格，**包含没 ready 的成员**。

| 成员 | 说明 |
|---|---|
| `revision` | 单调递增。传给 `changes(since=)` 可以接着往下看 |
| `members` | 全部占位者 |
| `ready()` | 其中 ready 的那些 |
| `slot(k)` | 座位 k 的占用者，空座返回 `None` |
| `get(identity)` | 精确到任期的那一个，不在返回 `None` |
| `len()` / 迭代 | 按成员数 |

`get()` 放在快照上而不是池子上是有意的：「那个 incarnation 还在吗」问的是**一个
时刻**，对着活池子问两次可能问到两个时刻。

---

## `Handle`

一个成员的引用。属性访问代理到对面的方法。

| 属性 | 说明 |
|---|---|
| `identity` | `"pool/座位#任期"`，也是围栏令牌 |
| `label` | 给人看的短形式 |
| `pool` / `id` / `slot` / `incarnation` / `url` / `state` / `ready` | 记录本身 |

```python
h.assign("task")                    # 调用
h.assign.timeout(5.0)("task")       # 单次调用的超时，默认 30 秒
```

超时做成修饰符而不是关键字参数，是为了不和对面方法的同名参数撞车。

`AsyncHandle` 是它的异步孪生：由 `apool()` 产出，方法返回 awaitable，其余完全
一样。

`hasattr(h, "assign")` 是**准的** —— handle 只代理这个池子真的提供的方法名。

---

## `Epoch`

冻住的一轮。

| 成员 | 说明 |
|---|---|
| `members` | 名单，冻结那一刻的 |
| `roster` | 指纹。各 rank 相同即名单相同 |
| `valid` | 占用者一变就是 `False` |
| `slot(k)` | 这一轮里的第 k 号 |

`valid` 在训练循环里查是没用的：卡住的 rank 根本到不了那一行。用后台线程 ——
NCCL 阻塞时会放开 GIL。

---

## `CallContext`

服务端侧的调用方身份。在参数上标注类型即可，库会填：

```python
def pull_job(self, ctx: tinyray.CallContext) -> dict:
    ctx.identity      # "worker/3#1874..."
    ctx.pool          # "worker"
    ctx.slot          # 3，无座位则 None
    ctx.incarnation   # 任期号
```

**自称的身份，不是认证。** 这个系统里任期号本来也是成员自己生成的。它买到的是
"调用方不会忘了传、也不会传错"，仅此而已。

---

## 异常

```text
TinyrayError
├── Unreachable          没拿到答复
│   ├── NotDelivered     确实没送到 —— 方法一定没跑，直接重试
│   └── OutcomeUnknown   可能跑了 —— 带 request id 重试，或保证幂等
├── Fenced               送到了，但那个座位换人了
├── RemoteError          送到了，对方的方法抛了（.type/.message/.traceback）
├── Stale                和注册中心失联，名单不可信
└── SeatTaken            座位被人占着（exclusive）或被更晚的任期拿走

NotFound(LookupError)    没人匹配
PolicyError(ValueError)  策略、座位、size 组合不成立
OversizeWarning(UserWarning)   超过 1 MB 提示线（只是提示，东西照送）
```

**只有 `NotDelivered` 可以盲目重试。** `OutcomeUnknown` 意味着对面可能已经做过
一遍 —— 这是唯一需要 request id 的情形。`RemoteError` tinyray 绝不替你重试，
能不能重做只有你知道。

`NotDelivered` 和 `OutcomeUnknown` 都是 `Unreachable` 的子类，所以既有的
`except Unreachable` 不受影响。

---

## 环境变量

| 变量 | 作用 |
|---|---|
| `TINYRAY_REGISTRY` | 注册中心地址，默认 `127.0.0.1:8760` |
| `TINYRAY_ADVERTISE` | 对外地址。多网卡机器上必须指定，否则可能登记错网卡 |
| `TINYRAY_SLOT` / `RANK` / `SLURM_PROCID` / `OMPI_COMM_WORLD_RANK` | 座位号 |
| `TINYRAY_SIZE` / `WORLD_SIZE` / `SLURM_NTASKS` / `OMPI_COMM_WORLD_SIZE` | 规模 |

---

## 注册中心

```bash
tinyray --listen 127.0.0.1:8760 --ttl-ms 20000
```

`--ttl-ms` 是租约长度，下限 200 毫秒（客户端按 ttl/4 心跳，再短会在两拍之间过期）。

**它只决定两件事：多久判定一个成员失联，以及心跳流量有多大。它不决定变化多久
可见。** 早先这三件事绑在同一个数上 —— 心跳间隔是 `ttl/4`，而那也是发现延迟的
上界，于是「不想误判」和「要及时」直接冲突。

现在注册中心在没话说的时候会**把应答挂起**，池子一动立刻返回。客户端请求挂起的
时长正好是它本来要睡的那个间隔，所以请求数量不变，但答复从「定时返回」变成
「有事就返回」：

| ttl | 旧的发现延迟上界 | 实测发现延迟 | 心跳/秒 |
|---|---|---|---|
| 2 s | 500 ms | 21 ms | 2.06 |
| 8 s | 2,000 ms | 19 ms | 0.75 |
| 20 s（默认） | 5,000 ms | 21 ms | 0.44 |

自己发布状态不受影响，一直是即时的（实测 0.6 毫秒）—— 有东西要发时，在途的挂起
请求会被取消掉重发。

不要求挂起的调用方（包括任何早于这个字段的客户端）照旧立刻得到答复。

| 端点 | 用途 |
|---|---|
| `GET /health` | 存活探针 |
| `GET /v1/pools` | 每个池子的 version / roster / 人数 |
| `POST /v1/beat` | 心跳（客户端用） |
