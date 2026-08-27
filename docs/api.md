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

    !!! warning "只写主机名，不要写 scheme 或端口"
        `http://` 和端口是围着它拼上去的，所以 `TINYRAY_ADVERTISE=http://10.0.0.5`
        会拼成 `http://http://10.0.0.5:33097`。这类写法现在**当场拒绝**并说明
        原因 —— 以前会登记成功，等到有人来调用才炸。

        要登记完全不同的地址（比如在反向代理后面），用 `join(url=...)`。

---

## `Member`

这个进程自己的注册。

### 属性

| 属性 | 说明 |
|---|---|
| `identity` | `"pool/座位#任期"`，和别人手里 Handle 上的那串一致 |
| `pool` / `slot` / `incarnation` | 分解开的同一件事 |
| `state` | 当前发布出去的 state（副本） |
| `is_ready` | 此刻对外宣称的就绪状态 |
| `accepted` | `False` 表示座位已被更晚的任期拿走 |
| `silence_ms` | 距离上一次成功心跳多久。它涨的时候一切照常，只是失效检测变慢 |
| `last_error` | 最近一次心跳失败的原因，恢复后仍保留 |
| `stats()` | 计数器，见下 |

### `stats()`

| 键 | 含义 |
|---|---|
| `beats_ok` / `beats_failed` | 心跳被应答的次数，和没有的次数 |
| `interval_ms` / `silence_ms` | 当前心跳间隔；距上次成功多久 |
| `watch_wakeups` | 本地缓存动过、并因此唤醒等待者的次数 |
| `short_polls` | 心跳等在**定时器**而不是注册中心上的次数。只有第一次 ack 之前才该发生；一直涨说明这个客户端在轮询，没吃到长轮询的好处 |
| `state_bytes` | 这个成员正在发布的 state 有多大 |
| `pool_revision` | 自己所在 pool 的版本号，以最后一次听到的为准 |
| `watched_pools` | 订阅了几个 pool |

**只有传了 `serves=` 的成员**才多出下面这些：

| 键 | 含义 |
|---|---|
| `calls` / `failed` | 处理过几次调用，其中几次抛了 |
| `refused` | 因为到并发上限被挡回去几次（503）|
| `in_flight` / `peak_in_flight` | 此刻在飞几个，历史峰值多少 |
| `busy_ms` | 花在处理函数里的总时间 |
| `concurrency_limit` | `max_concurrency` 的值 |

这一半存在，是为了让"要不要给长调用单开一条通道"有答案而不是有观点。
`max_concurrency` 挡的是无限堆积，**不是隔离**：并发槽被占满之后，control 调用
和别的调用一样吃 503。`refused` 和 `peak_in_flight` 摆在一起看，就知道这件事是
不是正在发生。

### 发布状态

```python
# 同时声明就绪 —— 属于决定"这个成员能不能用"的那部分代码
me.ready(**state) -> Member          # 合并进已有 state，并标记 ready
me.set_ready(state: dict) -> Member  # 整体替换，并标记 ready
me.unready() -> Member               # 保留 state，但标记为不可用

# 只发布状态，不碰就绪 —— 属于其余所有代码
me.update(**state) -> Member         # 合并
me.replace(state: dict) -> Member    # 整体替换

me.flush(timeout=10.0) -> Member     # 阻塞到注册中心确实收下
```

`ready()` 和 `update()` 都是**合并**，发出去的 key 拿不回来 —— 要清掉用
`set_ready()` 或 `replace()`。

!!! warning "上报进度请用 `update()`，不要用 `ready()`"
    `ready()` 一次断言两件事：这是我的状态，而且我可用。对于决定就绪的那部分
    代码这正合适；对别的代码就是越权。

    只报进度却调用 `ready(step=n)`，会把另一处刚下的暂停静默掀掉 ——
    `unready()` 之后一句 `ready(step=1)`，对端看到的 ready 就从 `False` 变回
    `True`，而调用者根本没打算表达这个意思。

    分开之后，"每个 Member 只有一个 readiness owner" 不再是一条要靠自觉遵守的
    约定：其余代码调用 `update()`，结构上就碰不到就绪位。

同值发布不花任何代价：state 和就绪位**都**没变时，既不会敲醒心跳，也不会抬高
池子版本。比的是解析后的值，不是字节 —— `{"b": 2, "a": 1}` 和 `{"a": 1, "b": 2}`
是同一件事，按字节比反而会当成两次改动白跑一趟。就绪位算在里面，所以
`unready()` 之后用同一份 state 再 `ready()` 一定会发出去。

!!! note "并发发布的顺序保证来自锁，不是 GIL"
    所有发布路径（`ready` / `set_ready` / `unready` / `update` / `replace`）都在
    同一把 `Member` 锁下完成"读旧值—合并—写入"，写入本身又在 Rust 侧的一把锁下
    完成。所以两个线程的发布是串行的，先拿到锁的那个先生效。

    GIL 保证不了这件事：它只让单条字节码不并行，管不到网络发送和完成的顺序。

    发出去的是**当前值**，不是一条日志。心跳只有一个循环，读的是那个寄存器，
    所以线上不会乱序；但两次发布挨得比一拍还近时，中间那个值可能根本不上线 ——
    这是软状态的定义，不是缺陷。要每一步都留痕，那是数据面的事。

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
pool.changes(since=None, timeout=None) -> Watch        # 迭代产出 Snapshot
apool.achanges(since=None, timeout=None) -> AsyncWatch # 异步迭代
```

`changes()` 阻塞在事件上，**不轮询**。池子不动就不返回。

流有三种结束方式，**其中一种会抛**：

| 结束原因 | 表现 |
|---|---|
| `timeout` 到了 | 循环正常退出 |
| 有人调了 `close()` | 循环正常退出 |
| **本进程被顶替** | 抛 `Fenced` |

前两种是"没事了"，第三种是"出事了"：座位没了，本地缓存从此**冻结**，之后每一次
查询都是陈旧的却不声张。三种都安静结束的话，丢座位和"超时到了、一切正常"长得
一模一样 —— 只能事后去查 `Member.accepted`，而那正是不该让调用方去猜的东西。

### 只盯几个字段

```python
pool.changes(fields=["role", "ready"])
apool.achanges(fields=["role", "ready"])
```

给了 `fields` 之后，只有这些字段（或成员进出、座位换人）真的动了才产出快照。
`ready` 和 `url` 是成员自己的一部分，也能点名；其余名字在 state 里找。

比较发生在 **Rust 缓存里**，在任何东西被序列化之前。实测 5,000 成员：

| | |
|---|---|
| `pool.snapshot()` | 8.78 ms |
| `field_digest(["role", "ready"])` | **0.40 ms** |

放在 Python 层做 predicate 是省不下来的 —— 它要拿到 `Snapshot` 才能判断，而那
时候钱已经花完了。

**身份永远算数。** 座位换人即使新任期发布的字段和前任一模一样，也会产出 ——
否则你会继续对着一个已经死掉的 incarnation 说话。

### `Watch` / `AsyncWatch`

`changes()` 和 `achanges()` 返回的对象，可迭代、可关闭、也是上下文管理器：

```python
with pool.changes() as w:          # 离开 with 即关闭
    for snap in w:
        ...

w = pool.changes()
w.close()                          # 也可以从另一个线程/任务关掉
```

**`close()` 是唯一能停下一个阻塞中 watcher 的办法。** 它等在事件上、而不是停在
`yield` 上，所以既 `close()` 不了生成器，设标志位它也看不见。`close()` 会顺手
敲一下铃，把它拽回到能看见标志位的地方。

`leave()` 会关掉所有还活着的 watcher —— 否则一个非 daemon 线程里的 watcher 会
让进程再也退不出去。

!!! note "异步侧不占用线程"
    `achanges()` 等在一个心跳会写入的管道上，由事件循环 select，**不借用
    executor 线程**。所以取消是即时的，取消多少个都不会影响别处。

    早先的实现用 `asyncio.to_thread`：取消 awaitable 并不会停掉底下那个线程。
    24 核机器上取消 40 个 watcher 之后，紧接着一次 `asyncio.to_thread` 要等
    3,092 毫秒 —— 默认 executor 的 28 个 worker 全卡在里面。

!!! info "为什么是快照流，不是事件流"
    客户端以心跳为采样率，注册中心会把一个间隔内的多次变化折叠成"该成员的当前
    状态"。所以承诺"不丢事件"是协议兑现不了的；快照能诚实兑现"不丢状态"。
    事件是一次 diff 的事 —— 每条记录都带 `incarnation` 和 `ready`。

### 等条件

```python
pool.until(predicate, since=None, timeout=None, describe="") -> Snapshot
await apool.auntil(predicate, since=None, timeout=None, describe="")
```

阻塞到 `predicate(snapshot)` 为真，返回那个快照；超时抛 `TimeoutError`，
`describe` 是错误里说"在等什么"的那句话。

**每个手写的等待循环都要做对同样四件事**，所以只写一遍：先看已经成立没有、把
revision 无缝交接过去、被 `close()` 时停下、`Fenced` 放出去而不是当成"条件还没
满足"。第二件做错最难发现 —— 池子在"先看一眼"和"开始订阅"之间动了，等待就会为
一个立刻成立的条件白等满整个超时。

下面几个都是它的特例。

### 等成员就绪（异步）

```python
await apool.await_ready(count=1, timeout=30.0, **filt) -> list[Handle]
```

`Pool.wait()` 的事件循环版。

!!! warning "不要在事件循环上调用继承来的 `wait()`"
    `AsyncPool` 继承了同步的 `wait()`，它在 loop 上不是"不够优雅"，是**停掉整个
    loop**：实测一秒的 `apool.wait()` 只放过 5 次 10ms 的 tick，本该有一百次。

    用 `asyncio.to_thread` 包一层也不对 —— 取消它并不会停掉底下那个线程，等待
    期间一直占着默认 executor 的一个 worker。

### 等指定任期离场

```python
pool.wait_departure(identity, timeout=None) -> bool
await apool.await_departure(identity, timeout=None) -> bool
```

阻塞到这个**任期**不在池子里了，返回 `True`；超时返回 `False`。离开、租约过期、
座位换人都算。

和 `wait_replacement()` 是两个问题：后者只在有人接任时才回答，前任只是走了、没人
接手的话，它会等满超时返回 `None`。要接手工作的一方通常只需要知道前任不在了。

### 等待座位换人

```python
pool.wait_replacement(slot=None, identity=None, timeout=None) -> Handle | None
await apool.await_replacement(slot=None, identity=None, timeout=None)
```

阻塞到这个座位由**另一个任期**接管，返回接任者的 `Handle`；超时返回 `None`。
`slot=` 和 `identity=` 二选一。

`Member.wait_fenced()` 是同一个问题的自视角，给必须放手的那个进程用；这个是
旁观视角，给正在跟它说话的人用。座位空着、座位换人、成员只是不再 ready 是三件
不同的事，只有 incarnation 分得清。

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

## `RegistryInfo`

注册中心是**另一个进程**，可以和 Python 包分开升级。`tinyray.__version__` 说的是
本地这一侧，对面能做什么得单独问：

```python
me.registry            # -> RegistryInfo
me.registry.protocol   # 只增不减的整数；老到不报的读作 0
me.registry.version    # 对面的版本号，用来写进日志
me.registry.supports("long_poll") -> bool
```

`RegistryInfo.FEATURES` 是功能名到所需 protocol 的对照表，放在依赖它的这一侧，
所以老客户端不需要认识将来的功能。功能名写错会抛 `ValueError` 而不是返回
`False` —— 后者会让一个笔误安静地走进降级分支。

不加入也能看：

```console
$ curl -s http://registry:7000/health
{"status":"ok","version":"0.9.0","protocol":1}
```

| protocol | 含义 |
|---|---|
| 0 | 长轮询之前（0.7.0 以前） |
| 1 | 认 `hold_ms`：没话说时挂起应答，被订阅的池子一动就立刻回 |

!!! warning "版本不匹配是性能悬崖，不是报错"
    老注册中心对长轮询请求的回答**又快又对**，只是不挂起 —— 所以"挂起了但什么
    都没发生"和"根本不会挂起"从客户端看一模一样，靠探测属性是猜不出来的。

    实测对着 0.6.1 的注册中心：每秒 **14.5** 次请求，而当前版本 **0.12** 次；
    发现延迟从一个往返退回一个心跳间隔。一切照常工作，没有任何东西会报错。

    所以 `join()` 在这种情况下会发一条 `OldRegistryWarning`。照常关掉：

    ```python
    warnings.filterwarnings("ignore", category=tinyray.OldRegistryWarning)
    ```

---

## `CallContext`

服务端侧的调用方身份。在参数上标注类型即可，库会填：

```python
def pull_job(self, ctx: tinyray.CallContext) -> dict:
    ctx.identity      # "worker/3#1874..."
    ctx.pool          # "worker"
    ctx.slot          # 3，无座位则 None
    ctx.incarnation   # 任期号
    ctx.request_id    # 调用方给这一次尝试起的名字
```

**自称的身份，不是认证。** 这个系统里任期号本来也是成员自己生成的。它买到的是
"调用方不会忘了传、也不会传错"，仅此而已。

`request_id` 默认每次调用都不同，两侧的日志因此能指着同一次尝试说话。

要让**重试共用一个名字**（幂等场景需要），把重试循环整个包起来：

```python
with tinyray.request_id(f"commit-{batch}"):
    for _ in range(3):
        try:
            return h.commit(rows)
        except tinyray.NotDelivered:
            continue
```

用块而不是逐调用的参数，因为重试本来就是块的形状，也因为关键字会和被调方自己的
参数名打架。ContextVar 实现，所以它跟着 `await` 走进这个块起的任务，不会漏进
旁边那个。

!!! note "tinyray 不做幂等缓存"
    只给名字，不按它去重。被调方无从知道一次调用重放是否安全 —— 结果留多久、
    什么算"同一次调用"，都是应用层的问题。这个决定属于调用方，`NotDelivered`
    （确定没送到，可以重试）和 `OutcomeUnknown`（可能已经跑了）的区分就是为了
    让它做得出来。要幂等就用这个 id 当键，自己实现。

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
TypeError                参数装不进被调方法的签名 —— 方法没跑，是调用方写错了
PolicyError(ValueError)  策略、座位、size 组合不成立
OversizeWarning(UserWarning)     超过 1 MB 提示线（只是提示，东西照送）
OldRegistryWarning(UserWarning)  注册中心比本包旧，某个功能不可用
```

**只有 `NotDelivered` 可以盲目重试。** `OutcomeUnknown` 意味着对面可能已经做过
一遍 —— 这是唯一需要 request id 的情形。`RemoteError` tinyray 绝不替你重试，
能不能重做只有你知道。

分到哪一类，看的是**被调方把这个请求读完了没有**，不是它回了什么状态码。它在读完
之前放弃的每一条路 —— content-length 读不懂、body 发到一半停住、并发到顶 ——
方法都还没被调用过，所以都是 `NotDelivered`。

参数装不进签名（位置参数太多、少给必填、关键字名字不认识、同一个参数给两次、类型
不匹配）也一样没跑过，但那是**调用方写错了**，所以走 `TypeError` 而不是
`Unreachable`：重试同样的调用不会有别的结果。

`NotDelivered` 和 `OutcomeUnknown` 都是 `Unreachable` 的子类，所以既有的
`except Unreachable` 不受影响。

---

## 环境变量

| 变量 | 作用 |
|---|---|
| `TINYRAY_REGISTRY` | 注册中心地址，默认 `127.0.0.1:8760` |
| `TINYRAY_ADVERTISE` | 对外**主机名或 IP**，只写这一个东西。多网卡机器上必须指定，否则可能登记错网卡 |
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
