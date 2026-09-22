# API 参考

对着实现写的，不是对着计划写的。每个签名都以 `python/tinyray/` 里的为准。

---

## 模块

| 名字 | 是什么 |
|---|---|
| `tinyray.join(...)` | 报到，返回 `Member` |
| `tinyray.pool(name)` | 拿一个 `Pool` |
| `tinyray.apool(name)` | 拿一个 `AsyncPool`（方法返回 awaitable） |
| `tinyray.Call(method, args=(), kwargs=None)` | 描述一个批量 RPC 项 |
| `tinyray.batch(handle, calls, timeout=30.0)` | 在一个成员上按顺序执行一批调用 |
| `tinyray.abatch(handle, calls, timeout=30.0)` | 批量调用的异步形式 |
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
    registry_url: str | None = None,
    coalesce_ms: int = 50,
) -> Member
```

阻塞到第一拍落地。联系不上抛 `Unreachable`，座位被更晚的任期占着抛 `SeatTaken`，
池子形状对不上抛 `PolicyError`。**一个进程只能加入一个 pool。**

加入失败会先关闭已创建的心跳和方法服务器，再抛出错误；同一进程可重试，不会留下
未注册但仍能接收调用的服务。

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
- **`coalesce_ms=`** —— 持续变化时成员通信的合并等待预算，单位毫秒，默认仍为 50。
  较小值降低突发变化的通知延迟，但会增加请求数；0 关闭这项间隔限制。只接受
  非负整数，实际间隔不超过租约的四分之一，避免大值阻碍续租。
  本地发布可提前唤醒正在休息的客户端。
- **`url=`** —— 手工指定原生方法端点（`host:port`）。默认由路由表探出 host，
  再加上 native listener 选出的端口；多网卡机器上可以用 `TINYRAY_ADVERTISE`
  指定 host。
- **`registry_url=`** —— 去哪个注册中心报到，压过 `TINYRAY_REGISTRY`。
  **别和上面的 `url=` 搞混**：那个是"别人怎么找到我"，这个是"我去找谁"。
  环境变量仍是常规通道（launcher 一次给所有 rank 设好）；这个参数是给用不了
  它的调用方 —— 嵌在别人进程里的库，为了配置一次调用去改 `os.environ`，
  改的是整个进程，而且比这次调用活得久。
  它选一个注册中心，不是加一个：一个进程一个成员一个注册中心，`pool()` /
  `apool()` 跟着 `join()` 走。给一串地址会被**当场拒绝**。

    !!! warning "TINYRAY_ADVERTISE 只写主机名"
        listener 的端口会自动加上。scheme、路径和端口都会被拒绝。要登记另一条
        原生 TCP 端点，用 `join(url="host:port")`。旧 `http://...` 方法地址会
        被明确拒绝，没有兼容 fallback。

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
| `coalesce_ms` / `effective_coalesce_ms` | 请求的合并等待预算；受当前租约四分之一限制后的预算 |
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

    发出去的是**当前值**，不是一条日志。协议 2 给 state、就绪位和 URL 一起加上
    发布序号；已取消的旧请求晚到时只续租，不覆盖新状态。两次发布挨得比一拍还近时，
    中间那个值仍可能根本不上线 —— 这是软状态的定义，不是缺陷。要每一步都留痕，
    那是数据面的事。

`flush()` 等待的是最新本地发布的确认，而不只是又一拍。联系不上会抛 `TimeoutError`，
座位被抢会抛 `SeatTaken`。防止确认后的回滚需要
`me.registry.supports("publication_ordering")`；老注册中心不能执行序号检查，
客户端会发出 `OldRegistryWarning`。

!!! note "state 是硬上限"
    16 KB，超了直接报错。它和 RPC 的体积限制不一样：state 会复制给**每一个
    订阅者**，实测 6 MB 到 20 个订阅者变成 120 MB。这条限制保护的是别人。

state 必须是有效 JSON：`NaN` 和无穷值抛 `ValueError`。发布被拒时，之前的状态和
就绪位保持不变。

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
大整数不会先舍入成浮点数再判断相等。

只含顶层标量字段（`null`、布尔、字符串、整数、有限浮点数）的重复过滤会使用
有界原生结果索引。嵌套对象、数组、超限键和其他不支持形状仍走同一条扫描路径，
结果语义不变。每个 pool 最多保留 32 个过滤、每个过滤最多 8 个字段和 4 KiB
键数据、每项最多 8,192 个匹配 ID，总估算内存不超过 1 MiB。成员 state/readiness
变化、移除、full resync 或注册中心重启都会使相关结果失效。索引只存 wire ID，
不存 Handle，也不复制 state。

`slot()` 使用原生座位索引，`pick()` 在序列化之前选出一个匹配者，不再为了返回
一个句柄构造整份 Python 名单。`all()`、`snapshot()`、`epoch()` 和内置等待则直接
拿 Rust 的不可变名单视图，不再把整份 roster 编成 MessagePack、交给 Python 解码。
每个 pool 最多缓存 all/ready 两份视图，里面只有 `Arc<Member>` 和索引，不复制
可能很大的 state。整份名单第一次读取 state 时，Rust 只编码一份有界
MessagePack state 数组，Python 也只解码一次，再按索引把互相独立的对象交给各
Handle；小于 1 MiB 的编码结果随不可变名单视图缓存，成员变化会连同视图一起失效。
修改一个句柄仍不会污染下一次查询或另一份快照。

数字这条规则一路走到底：`cfg={"shard": 6/2}` 同样找得到发布
`cfg={"shard": 3}` 的人，数组里也一样。形状仍然精确 —— 嵌套对象要键完全
相同、数组要顺序和长度都相同，放宽的只有数字本身。

### 快照与变化

```python
pool.snapshot(include_unready=True) -> Snapshot
pool.changes(since=None, timeout=None) -> Watch        # 迭代产出 Snapshot
apool.achanges(since=None, timeout=None) -> AsyncWatch # 异步迭代
```

`changes()` 阻塞在事件上，**不轮询**。池子不动就不返回。
超时限制整个流的生命周期，即使变化持续到来也会按时结束。

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

比较发生在 **Rust 缓存里**，在任何东西被序列化之前。`snapshot()` 本身现在也只
创建一个共享原生视图；只有访问 `members` 或开始迭代时才批量创建 Python
`Handle`。所以 `len(snapshot)`、repr、`slot()`、`get()` 和字段摘要都不再付整份
名单的物化成本。

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
    `achanges()` 等在一个 cache/lifecycle 变化才会写入的管道上，由事件循环
    select，**不借用 executor 线程**。空 heartbeat ack 不写管道，所以安静池不会
    按心跳频率反复唤醒。取消是即时的，取消多少个都不会影响别处。

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

任意 Python predicate 仍走这条通用路径；predicate 不能搬进 Rust。下面几个内置
条件不再是它的 Python 特例：`wait()`、`wait_departure()`、
`wait_replacement()` 和 `epoch()` 把计数、身份、指纹与超时交给 Rust，在同一把
revision/condvar 上无缝检查并等待。异步版本复用事件循环管道，不借 executor
线程，也没有 sleep/poll 循环。

heartbeat、publication ACK 与 discovery revision 使用独立通知域：续租成功但名单
没动时只唤醒注册/发布等待，不触碰任何 discovery waiter。首次注册同样直接等到
绝对 deadline，不再每 100 ms 醒来检查一次。

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

确认自己被顶替后，已有 `Epoch.valid` 变为 `False`，再调用 `epoch()` 抛 `Fenced`。
仅仅失联不会中断已经运行的一轮。

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

`members` 仍是不可变 tuple，但它是懒的：创建快照不会创建任何 `Handle`；第一次
访问 `members` 或迭代时才物化一次并缓存。`ready()` 只物化 ready 的成员。
快照持有旧 `Arc<Member>`，所以后续发布、离场或注册中心重启都不会改写它。

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

从 Pool/Snapshot/Epoch 返回的 Handle 内部持有 `Arc<Member>` 的原生引用。构造几千个
Handle 时不会复制 state；`pool`、身份、座位、任期、URL、ready、identity、label
和方法代理都直接读原生引用。第一次访问 `state` 才创建一份独立 Python dict，并
安装到真实 slot；同一名单中的 Handle 共享一次批量 decode，后续读取不再经过
property 或 native 调用。修改它不会污染其他 Handle、缓存或冻结快照。手工
`Handle(pool, raw, methods)` 的兼容构造方式和字段可写语义不变。

```python
h.assign("task")                    # 调用
h.assign.timeout(5.0)("task")       # 单次调用的超时，默认 30 秒
h.pull_job.returns(AgentJob)()      # 把 MessagePack 结果恢复成 AgentJob
```

`url` 是裸 `host:port`。方法连接持久并可多路复用：每条连接只有一条串行 frame
写路径和一个 reply reader，最多同时承载 128 个按 request ID 相关的调用，回复可
乱序到达。每端点最多 256 个本地在途调用、4 条连接；每进程最多 512 个在途调用、
256 条客户端连接，并只保留 64 条 idle 连接。选择时取当前负载最小的连接；所有
现有连接都已有两个预留调用时，就继续开连接直到端点的 4 条上限。这样普通并发下
writer/reader 队列保持很浅，又不会退回一调用一 socket。每帧都是 `u32` 大端长度
加一张 MessagePack map：

Python 与嵌入式 Rust 调用共用这一套公开 `tinyray::Client` 实现。PyO3 层只适配
Python 值、completion 投递和取消，不再维护第二份连接池、socket reader/writer
或 reply routing 状态机。

```text
call  = {v: 1, id, from, to, op: "call", method, body: bytes}
batch = {v: 1, id, from, to, op: "batch", batch: count, body: bytes}
reply = {v: 1, id, status, body: bytes, error?, batch_index?, completed?}
```

`body` 对 Rust 是 opaque bytes，里面是一份单独编码的 application value。reply
status 是 `success`、`method_not_found`、`fenced`、`caller_fault`、
`concurrency_refused`、`remote_error`、`malformed_protocol`、`internal`。
畸形、截断、超限、重复或未知 reply ID 会毒掉连接，并按各请求的 frame 是否完整
写出给所有剩余调用分类。完整写出后超时或取消只移除自己的 waiter；已知 request ID
的迟到回复会被丢弃，不会毒掉别的调用。没有 HTTP/JSON listener 或兼容 fallback。
方法 frame 的硬上限是 32 MiB，长度前缀会在分配 body 之前检查；连接数、在途
frame 数及大小 frame 的字节数同时受 process-global 和 per-server 准入预算限制，
小于等于 1 MiB 的控制消息使用独立预算，不会被少数最大 frame 挤掉。
`max_concurrency` 仍在进入 Python 方法前逐请求限流；batch 只占一个槽，内部仍串行。

客户端池里的连接最多空闲复用 10 秒。服务端只在没有活跃调用时按 15 秒 idle
关闭连接；一旦 frame 开始，prefix/body 共用一份绝对 15 秒截止时间。这个 5 秒
余量让客户端先丢弃过期池项，服务端也不会为永远沉默的 peer 永久保留 task。

标准 dataclass 可以直接作为参数和返回值：

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Request:
    prompt: str
    max_tokens: int

@dataclass(frozen=True)
class Reply:
    text: str
    tokens: int

class Worker:
    def infer(self, request: Request) -> Reply:
        return Reply("done", request.max_tokens)

reply = h.infer.returns(Reply)(Request(prompt="hello", max_tokens=32))
```

调用端 dataclass 先变成 MessagePack map；服务端按方法参数注解直接恢复。方法返回
dataclass 时会自动编码；调用端不写 `.returns(T)` 得到普通 dict/list，写了就恢复
成本地声明的类型。dataclass 可以嵌套，也可以放进 `list[T]`、`dict[K, V]`、
Optional/Union、tuple 和 set。`NamedTuple`、`TypedDict`、Enum、datetime、
date/time/timedelta、UUID、Decimal 和这些容器同样递归恢复。

0.18 **刻意移除了 Pydantic 集成**：没有 `tinyray[pydantic]` extra，没有
`BaseModel`/Pydantic dataclass 的特殊编码，也没有 alias、validator、serializer
或 strict marker。把 Pydantic 对象直接作为参数会在发送前抛 `TypeError`；
`.returns(PydanticType)` 也会在调用前明确拒绝。需要它时，应用先显式转换成标准
dataclass、TypedDict 或普通 MessagePack 值。

每个成功的 native reply 已经把结果放在 opaque application payload 里；没有
`/_result/` 路径、HTTP status 或 fallback 请求。能直接恢复的模型参数从
`msgspec.Raw` MessagePack 子片段构造，避免先建通用 dict/list 对象图。

MessagePack array 不保留普通输入原本是 list、tuple 还是 set。`.returns(T)`
在调用端声明要恢复成什么：

未声明类型的值遵循 `msgspec.msgpack`：bytes 与 datetime 原样保留；UUID 和 Enum
变成各自的 wire value；tuple/set 变成 array；整数和 tuple map key 保留；
NaN/Infinity 仍是 float。超出 MessagePack 64-bit 有符号/无符号范围的 Python int
使用保留 ext code 121；122、123、125、126 保留大整数 map/set 与复合
tuple/frozenset key。这些 code 不携带 Python 类名；应用不要直接使用这些 code 的
`msgspec.msgpack.Ext`。

```python
class AgentJob(NamedTuple):
    attempt: AttemptKey
    proxy_url: str

job = h.pull_job.returns(AgentJob)()
jobs = await ah.pull_jobs.returns(list[AgentJob])()
```

转换失败抛本地 `TypeError`，消息带远端身份、方法名和出错路径；远端方法此时已经
成功运行，失败只发生在结果恢复阶段。协议不会在线上传 Python 类名。只有标准
dataclass 和上述类型化容器是明确支持的模型边界；其他任意 Python 对象会被拒绝。

`.returns()` 和 `.timeout()` 都是单次调用的修饰符，可以任意顺序组合：

```python
h.pull_job.timeout(5).returns(AgentJob)()
h.pull_job.returns(AgentJob).timeout(5)()
```

修饰符不用关键字参数，是为了不和对面方法的同名参数撞车。

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

和 `Snapshot` 一样，`members` 是懒物化并缓存的不可变 tuple；`len()`、`slot()`、
`valid` 和 repr 直接读原生冻结视图。

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
me.registry.supports("publication_ordering") -> bool
me.registry.supports("native_registry") -> bool
```

`RegistryInfo.FEATURES` 是功能名到所需 protocol 的对照表，放在依赖它的这一侧，
所以老客户端不需要认识将来的功能。功能名写错会抛 `ValueError` 而不是返回
`False` —— 后者会让一个笔误安静地走进降级分支。

不加入也能看：部署探针连到 `host:port`，发一帧 `health` 操作。每帧都是
`u32` 大端长度加一张 MessagePack map：

```text
request  = {request_id: 7, operation: "health", payload: nil}
response = {request_id: 7, operation: "health_ack",
            payload: {status: "ok", version: "0.18.0", protocol: 3}}
```

这不是 HTTP 端点，也没有 curl 兼容层。

| protocol | 含义 |
|---|---|
| 0 | 长轮询之前（0.7.0 以前） |
| 1 | 认 `hold_ms`：没话说时挂起应答，被订阅的池子一动就立刻回 |
| 2 | 认 `publication`：旧请求不能覆盖新的 state、就绪位和 URL |
| 3 | 注册中心改用原生长度前缀 MessagePack 传输 |

0.18 对两条传输都是硬切换：注册中心和方法 RPC 都不再监听 HTTP/JSON，客户端
也不会回退到旧 wire。

!!! warning "缺失功能会影响性能或一致性"
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

Context 可以是普通参数、位置专用参数或关键字专用参数。其余参数保留 Python 的
绑定规则，包括 `*args`、`**kwargs`、默认值和重复参数检查。

**自称的身份，不是认证。** 这个系统里任期号本来也是成员自己生成的。它买到的是
"调用方不会忘了传、也不会传错"，仅此而已。

`request_id` 默认每次调用都不同，两侧的日志因此能指着同一次尝试说话。调用方
identity 太长时会保留可读前缀、加入 identity 的 SHA-256 和精确序号，始终不超过
200 字节；显式 `request_id()` 仍按原规则拒绝超长值。

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

## 批量 RPC

```python
calls = [
    tinyray.Call("assign", args=("task-1",)),
    tinyray.Call("assign", kwargs={"task": "task-2"}),
]
results = tinyray.batch(handle, calls)
results = await tinyray.abatch(handle, calls)
```

每批最多 128 项，全部发给同一个成员，顺序执行，第一项失败就停止。空批次是本地
空操作。它把多项操作合在一次 framed TCP 往返里，**不是事务，也不回滚**。

`BatchError` 包含从 0 开始的 `failed_index`、之前成功项的 `completed_results`
以及 `cause`（`RemoteError`、`TypeError`、`AttributeError` 或 `Fenced`）。
此前的项已完成；失败方法抛错前可能已有副作用；之后的项不会执行。每项结果在
执行下一项之前就被序列化，且每项之前重新检查 fencing。

传输超时针对整个批次交换，不是每项各享一份。`OutcomeUnknown` 表示不知道执行了
哪一段，也可能整批都执行了；异步取消只停止等待，不撤销远端执行。库不会自动重试，
也不会把旧 HTTP 服务悄悄降级成逐项调用；旧 URL 会被明确拒绝为不兼容端点。

每项的 `CallContext.request_id` 从批次 ID 和下标稳定派生。短 ID 形如
`<batch-id>:<index>`；长 ID 带确定性哈希，保持在 200 字符内。应用需要核对重试
结果时，用 `request_id()` 固定批次 ID；去重仍由应用负责。

一批占一个并发槽，`calls` 按一次请求计数；任一项失败则整次请求计入 `failed`。

## Rust SDK

公开 workspace crate `tinyray` 不依赖 Python/PyO3，主要类型：

- `MemberBuilder` / `Member`：join、发布 state/readiness、watch、flush、leave。
- `DiscoveryPool` / `Snapshot` / `MemberRef` / `Epoch`：直接读取 Arc-backed roster，
  提供 filter/count/pick/slot/get、冻结视图、有效性检查，以及同步/异步
  count/departure/replacement wait，不复制成员 state。
- `Service` / `Router`：发布具名方法并分派 opaque MessagePack payload。
- `CallContext`：caller identity、request ID、fencing target，以及连接/服务关闭时
  触发的 cancellation token。
- `Client` / `Target`：raw 与 serde typed 的同步/异步调用；request ID 和重试策略
  完全由调用方控制。
- `RpcRuntime`：让多个 `Client` / `Server` 共用一套 Tokio worker；高层
  `MemberBuilder` 默认把 client/server 放在同一个 4-worker runtime。
- `ReceivedRawReply` / `ReceivedRpcReply`：持有 response 的 BlobRef ACK guard；
  用 `decode<T>()` 完成映射，或在不再需要 raw bytes 时 drop。
- `Server` / `ServerConfig`：独立 listener 生命周期、准入上限、计数和显式关闭。

`Router::raw` 不反序列化应用 payload；`typed` 显式解整个 payload，
`typed_arg` / `typed_no_args` 使用与 Python 兼容的参数 envelope。batch 仍只占一个
准入槽，内部逐项串行执行，第一次失败即停止，并返回 `batch_index`、`completed`
和已成功前缀。

通用 listener 与 Python 共用。Rust `Service` future 直接在 Tokio 上运行，不拿 GIL，
也不进 `spawn_blocking`；Python `serves=` 在同一 listener 上安装
`PythonService` adapter，只有 adapter 会进 `spawn_blocking` 并获取 GIL。

## BlobRef

```python
with tinyray.blob(data) as blob:
    result = handle.consume(blob)
    view = blob.view()       # 只读 memoryview，不复制 payload
    copied = bytes(blob)     # 显式复制
```

`BlobRef` 是显式的同机传输，不是跨机器 fallback。Linux 创建端只把输入复制一次到
`memfd`，设置 0600/CLOEXEC，并封住 write/grow/shrink/继续修改 seals，随后只读
映射。`MAX_BLOB_BYTES` 是 Python 默认的 256 MiB 创建/接收上限；Rust 对应
`DEFAULT_MAX_BLOB_BYTES`，两端都可以按操作指定更小上限。MessagePack extension
**124** 只携带有界 descriptor。每个解码消息最多包含
`MAX_BLOB_REFS_PER_MESSAGE`（64）个 BlobRef 和
`MAX_BLOB_MAPPED_BYTES_PER_MESSAGE`（512 MiB）不重复映射；相同 descriptor
在消息内共享同一映射。原生兜底准入还把直接 serde/
`from_descriptor` 的存活 handle 限为 128、映射限为 64、总映射字节限为
512 MiB。descriptor 包含协议版本、Linux boot fingerprint、owner pid/fd、payload
大小、device/inode，以及由 Linux `getrandom` 产生并同时写在 sealed header 里的
随机 token。

接收端先检查默认 256 MiB 大小上限、boot identity 和数值 pid/fd，然后只按代码
构造并只读打开 `/proc/<pid>/fd/<fd>`；映射前核对 device、inode、文件总长、全部
必需 seals、header magic、token 和 size。过期、fd 复用、跨 boot、未封口、超限
或畸形 descriptor 都在调用方法前抛 `BlobError`。未知 MessagePack extension 仍是
普通 `msgspec.msgpack.Ext`，不能伪造 BlobRef。

调用参数的 owner 会移入原生 pending 状态；完整写出后即使 timeout/cancel，也要等
迟到 reply 或连接关闭才释放。服务端 response owner 则一直保留到调用方完成解码并
回送相关的 BlobRef acknowledgement。每个 reply 最多 64 个不重复 owner、512 MiB；
未确认 reply 还受 connection（128/512 MiB）、server（512/1 GiB）和 process
（2,048/2 GiB）总预算限制，重复 owner 只计一次，连接关闭会释放全部占用。raw
reply guard 存活时，两端不会触发通常的 10/15 秒 idle 回收；未确认 response
仍有 60 秒 server 硬截止时间。

每次公开序列化都写当前进程 PID 和本地 fd，因此 fork 子进程中仍有效的映射不会发送
父进程 descriptor。映射成功后，即使发送方 close、正常退出或 SIGKILL，接收方的
`BlobRef` 仍有效。最后一个 fd/mapping 关闭后匿名对象由内核自动删除。已有
memoryview 时普通 close 会被拒绝；fork 子进程会在遗忘继承 runtime 前关闭所有原生
登记 BlobRef 和仅由 pending/abandoned RPC 持有的 owner。

`BlobRef.from_descriptor(...)` 为显式协议集成与测试保留，但不接受路径，也不能绕过
identity、seal、完整 MessagePack 消费、size、count 或 mapped-byte 准入。Rust
`BlobRef::from_file` 使用位置读取，不改变调用方文件游标，并拒绝读取期间的截断或
增长。

非 Linux、没有 `/proc`、权限不符或跨机器时绝不悄悄退回普通 bytes。原有 bytes
编码与传输完全不变。

## 异常

```text
TinyrayError
├── Unreachable          没拿到答复
│   ├── NotDelivered     确实没送到 —— 方法一定没跑，直接重试
│   └── OutcomeUnknown   可能跑了 —— 带 request id 重试，或保证幂等
├── Fenced               送到了，但那个座位换人了
├── RemoteError          送到了，对方的方法抛了（.type/.message/.traceback）
├── BatchError           某项失败，携带下标、此前结果及原因
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
| `TINYRAY_REGISTRY` | 注册中心地址，默认 `127.0.0.1:8760`。只接受**一个**地址；`join(registry_url=)` 可以压过它 |
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

不要求挂起的调用方照旧立刻得到答复。

生产心跳连接持久复用，但严格串行：一条连接同一时刻只有一个 `beat` 在途，收到
完整且 request ID 匹配的 `beat_ack` 之后才能发下一项。发布取消长轮询、timeout、
EOF、坏帧、错 request ID、注册中心重启或拒绝都会丢弃连接，下一拍再懒连接，所以
迟到的回复不可能被下一拍读走。静默 35 秒的连接由服务端关闭。`health` /
`debug_pools` 探针仍是一问一答。

请求上限 512 KiB，回复上限 64 MiB，长度在分配 body 之前检查。所有回复都回显
`request_id`。health 还报告累计 accept、当前连接和已收 frame，供压测与排障。

| operation | reply | 用途 |
|---|---|---|
| `health` | `health_ack` | 存活、版本和 protocol 探针 |
| `debug_pools` | `debug_pools_ack` | 每个池子的 version / roster / 人数 |
| `beat` | `beat_ack` | 心跳、长轮询和 `leaving=true` 告别 |

未知操作和坏帧返回 `operation="error"` 的结构化错误；座位或池形状拒绝仍是正常
`beat_ack`，只是 `accepted=false`。
