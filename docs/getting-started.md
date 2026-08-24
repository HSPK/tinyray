# 上手

十分钟，从装上到两个进程互相找到、互相调用。

## 装

```bash
pip install tinyray
```

wheel 里带着注册中心，不用再装第二个东西。

```bash
tinyray --listen 127.0.0.1:8760 --ttl-ms 20000
```

它没有配置文件、没有数据目录、不写磁盘。所有状态都由成员每次心跳重新声明，
所以**它挂了重启就行** —— 一个心跳间隔之内自己就长回来了。

!!! tip "在集群里"
    注册中心是**一份**，由 supervisor 拉起来就够。不做副本是刻意的：它的状态
    是软的，丢了只是一段发现能力的空档，不是丢数据。

## 三个概念

| 概念 | 一句话 |
|---|---|
| **member** | 一个进程。`join()` 之后它在册，心跳停了它就出局。 |
| **pool** | 一组同类进程。按名字找。 |
| **incarnation**（任期） | 一次进程生命。同一个座位换了人，任期号就变。 |

一个进程加入一个 pool。要多种角色就多起几个进程，别把逻辑组件都注册成成员。

## 报到

```python
import tinyray

me = tinyray.join("collector", "stateful", slot=0)
me.ready(model_version=17)
```

`join()` 会**阻塞到第一拍落地**，返回即代表注册中心确实收下了你。联系不上会抛
`Unreachable`，不会假装成功。

`ready()` 是挂牌子：报的东西写进你的 state，别人查得到。

## 让别人能调你

把一个普通对象交给 `serves=`，它的公开方法就是接口 —— 不用装饰器，不用 IDL。

```python
import tinyray

class Collector:
    def assign(self, task: str) -> dict:
        return {"took": task}

with tinyray.join("collector", "stateful", slot=0,
                  serves=Collector(), max_concurrency=64) as me:
    me.ready(model_version=17)
    ...
```

`max_concurrency` 给并发封顶。超了直接拒绝而不是排队 —— 拒绝是有界的，排队不是。

底下就是普通 HTTP，所以 `curl` 排障的本事一点没丢：

```bash
curl -X POST http://host:port/call/assign -d '{"task":"t"}'
curl http://host:port/_methods
```

## 找人，然后调用

```python
import tinyray

me = tinyray.join("driver", "churn")
me.ready()

pool = tinyray.pool("collector")
engine = pool.wait(count=1, timeout=20)[0]

engine.assign("task-7")          # {'took': 'task-7'}
pool.pick(model_version=17)      # 按 state 过滤，随机挑一个
pool.slot(0)                     # 按座位号取，空座会抛 NotFound
```

**查询读的是本地缓存，不走网络。** 所以查多少次都不会给注册中心加压，也不会有
超时。缓存落后真相约一个 RTT（默认配置实测 21 毫秒）—— 注册中心是在变化发生时
把答复送回来的，不是等你下一拍来问。

## 谁在调我

方法签名里标一个 `CallContext`，库会替你填：

```python
class Collector:
    def assign(self, task: str, ctx: tinyray.CallContext) -> dict:
        # ctx.identity / ctx.pool / ctx.slot / ctx.incarnation
        return {"took": task, "for": ctx.identity}
```

调用方那边什么都不用写。

!!! warning "这不是认证"
    身份是**自称的** —— 这个系统里任期号也是成员自己生成的。它买到的是"调用方
    不会忘了传、也不会传错"，不是"对方无法伪造"。别拿它当权限边界。

## 调用失败了，能重试吗

这是最要紧的一件事。**只有"确实没送到"才能原样重试。**

```python
try:
    engine.assign("task-7")
except tinyray.NotDelivered:
    # 根本没到对面，方法一定没跑。换一个人重试就行，不需要 request id。
    ...
except tinyray.OutcomeUnknown:
    # 可能已经跑了。要重试就得带上同一个 request id，或者保证幂等。
    ...
except tinyray.Fenced:
    # 那个座位换人了。重新查地址再说。
    ...
except tinyray.RemoteError as e:
    # 送到了，对方的方法自己抛了。tinyray 绝不替你重试这个。
    print(e.type, e.message, e.traceback)
```

前两个都是 `Unreachable` 的子类，所以老代码里的 `except Unreachable` 照常工作。

| 异常 | 送到了吗 | 你该干嘛 |
|---|---|---|
| `NotDelivered` | **确实没有** | 直接重试 |
| `OutcomeUnknown` | **不知道** | 带 request id 重试，或保证幂等 |
| `Fenced` | 送到了 | 重新查地址 |
| `RemoteError` | 送到了 | 业务问题，你自己定 |

## 等变化，不要轮询

```python
snap = pool.snapshot(include_unready=True)

for snap in pool.changes(since=snap.revision):
    print(snap.revision, len(snap), len(snap.ready()))
```

`changes()` 阻塞在事件上，池子不动就一直不返回，**不烧 CPU**。异步用
`AsyncPool.achanges()`。

循环安静地结束，意味着超时到了或者有人调了 `close()` —— 都没事。但如果**本进程
被顶替**，它会抛 `Fenced`：那不是"看完了"，那是座位没了、缓存从此冻结，得停掉
手里的东西再重新 `join()`。

```python
try:
    for snap in pool.changes():
        ...
except tinyray.Fenced:
    ...   # 座位没了
```

`snapshot()` 和 `all()` 的区别值得记住：

- `all()` 回答"**谁能用**" —— 只给 ready 的
- `snapshot()` 回答"**谁在座**" —— 包括入座了还没 ready 的

准备阶段常见的坑就在这：成员已经占了座位（别人拿不走），但还没 ready，这时
`all()` 会说它不在。`snapshot()` 才是那个问题的答案。每条记录都带
`incarnation` 和 `ready`，所以比对两份快照就能分清"人还在只是没 ready"、
"原来那个走了"和"同一个座位换了人"。

## 被别人顶替了

座位是后来者居上 —— 重启的进程要能把自己的座位拿回来，哪怕旧的租约还没过期。
被顶替的一方需要主动知道，因为它手里可能还攥着 GPU、推理服务和自己的端口：

```python
if me.wait_fenced(timeout=60):
    shutdown_my_inference_server()   # tinyray 只挡得住走它的调用
```

异步用 `await me.await_fenced()`。

!!! warning "它防不了脑裂"
    要知道自己被换了，得**联系得上注册中心**。网络分区时这里会一直等下去 ——
    联系不上就无从得知。这和"注册中心挂了不停训练"是同一个取舍。

## 发布状态并确认

```python
me.set_ready({"weights": "v9"})   # 整体替换，不是合并
me.flush(timeout=10)              # 等到注册中心确实收下
```

`ready(**kw)` 是**合并**语义，发出去的 key 拿不回来。要换掉整张图就用
`set_ready()`。`flush()` 免得你发完再反查自己。

## 一起点名

要所有 rank 拿到**同一份名单**（比如建通信组），用 `epoch()`：

```python
me = tinyray.join("trainer", "collective", slot=RANK, size=WORLD_SIZE)
me.ready()

ep = tinyray.pool("trainer").epoch()   # 等人齐，然后冻住
build_process_group(ep.members)        # 每个 rank 拿到的必然一样
```

冻结的一轮带着一个指纹，各 rank 的指纹相同就代表名单相同 —— 因为一轮只有在
指纹恰好是它自己那份名单算出来的时候才会被交出去。

```python
def watchdog():                        # 训练循环里查是没用的：卡住的 rank
    while ep.valid:                    # 到不了那一行。后台线程可以，因为
        time.sleep(0.5)                # NCCL 阻塞时会放开 GIL。
    pg._abort()
```

## 走之前

```python
me.leave()
```

正常退出会自动 `leave()`，座位立刻空出来而不用等租约到期。被 SIGKILL 就退回
租约过期，两条路都通，只是快慢不同。

## 接下来

- [API 参考](api.md) —— 完整接口
- [为什么](01-why.md) —— 问题、代价、现有工具为何不合身
- [是什么](02-design.md) —— 策略、边界、规模与保证
