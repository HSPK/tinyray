# API

> 提案；当前未实现。以下签名描述的是目标设计，不是已安装的包。

## 1. 加入集群

```python
join(target, *, group, slot=None, rank=None, registry=None,
     readiness=None, meta=None, bind=None) -> Membership
```

在一个控制端口上服务 `target` 并注册它。**不阻塞** —— 控制端口跑在自己的线程上，因为
`__main__` 属于框架。

rank 来自 launcher（`RANK`、`SLURM_PROCID`、`OMPI_COMM_WORLD_RANK`），除非显式覆盖。
**不存在任何资源参数。**

| 成员 | 含义 |
|---|---|
| `Membership.slot` | 逻辑 Slot |
| `Membership.incarnation` | 本进程的 Incarnation |
| `Membership.state` | `Current` / `Superseded` / `Expired` |
| `Membership.endpoint` | 服务地址 |
| `Membership.leave()` | 注销；从不抛出 |

## 2. Identity

```python
Slot(kind, **coords) -> Slot
Slot.incarnate() -> Incarnation
on_superseded(callback) -> None
```

```python
slot = tinyray.Slot("collector", cell="c07", index=3)
str(slot)          # "collector/c07/3"
```

Slot 名从不编码位置。

## 3. Discovery

```python
group(name) -> GroupView
```

| 成员 | 返回 | 说明 |
|---|---|---|
| `.ranks(list)` | `GroupView` | 更窄的作用域 |
| `.shard(i, n)` | `GroupView` | 从 i 开始每 n 个取一个 |
| `.ready()` | `GroupView` | 仅已就绪成员 |
| `.members(fresh=False)` | 记录 | 除非 `fresh` 否则走缓存 |
| `[rank]` | 句柄 | 可调用，带 fencing |
| `len()`、迭代 | | |
| `.wait_ready(size, timeout)` | 自身 | 取代 gang placement |
| `.watch(callback)` | Watcher | 轮询 membership 版本号 |

响应大小由作用域决定，绝不由集群决定。

## 4. 调用

```python
handle.method.remote(*args, **kwargs) -> Reference
get(refs, *, timeout=300.0) -> Any
wait(refs, *, num_returns=1, timeout=300.0) -> (ready, pending)
release(refs) -> None
```

`.remote()` 在调用运行之前返回。`wait` 只问状态，不传输 payload。句柄可 pickle，因此一个
peer 引用可以发给第三个进程。

## 5. Readiness

```python
readiness(*predicates) -> Readiness
```

内置谓词：`ProcessAlive`、`PortOpen`、`HttpOk`、`LogMatch`、`QueueBelow`、
`EventLoopLagBelow`。

领域谓词属于应用。谓词返回 `bool` 或 `(bool, reason)`；否定裁决必须携带原因。

## 6. Admission

```python
admission(max_pending=1000, high_watermark=0.8, low_watermark=0.6) -> Admission
```

| 成员 | 返回 |
|---|---|
| `.try_admit()` | `Ticket` 或拒绝，**从不阻塞** |
| `.credits()` | 剩余容量 |
| `.depth()` | 当前深度 |

## 7. Reconciliation

```python
reconciler(desired, observed, fn, interval=2.0) -> Reconciler
leadership(name) -> 上下文管理器
Reconciler.publish(state) -> version
Reconciler.epoch(min_ready) -> Epoch
```

收敛函数必须幂等；它会被反复调用。

## 8. Supervision

```python
supervise(command, *, ready_when="alive", env=None, cwd=None,
          startup_timeout=600.0, stop_timeout=30.0) -> Process
```

没有 `num_gpus`，没有 `num_cpus`。`ready_when` 接受 `"alive"`、`"port"`、
`"http[:/path]"`、`"log:regex"`、一个谓词或一个可调用对象。

| 成员 | 含义 |
|---|---|
| `.is_alive()`、`.exit_code()` | |
| `.tail(n)` | 环形缓冲，200 行 |
| `.stop(timeout)` | 向进程**组**发信号 |

## 9. Registry

```python
serve_registry(bind, *, ttl=30.0, background=False) -> Server
```

无状态。跑若干个；它们之间不通信。

## 10. 异常

```
TinyrayError
├── ProtocolError
│   └── MessageTooLarge
├── RegistryUnavailable
├── NotLinked
└── RemoteCallError          .kind, .remote_traceback
    ├── UserCodeError
    ├── ObjectLost
    ├── NotFound
    ├── Fenced
    └── Backpressure

InsufficientCapacity
StartupError
NotLeader
ConsensusUnavailable
```

## 11. 从旧 API 中移除的部分

| 移除 | 替代 |
|---|---|
| `init(num_cpus=, num_gpus=)` | 无；不存在资源表 |
| `remote(num_gpus=...)` | `join()`；设备由 launcher 分配 |
| `create_actors(count=...)` | 由 launcher 拉起；用 `wait_ready(size)` |
| `launch_workers(gpus_per_worker=, cpus_per_worker=)` | 由 launcher 拉起，且已定好规格 |
| `link(**groups)` | 自注册与作用域 `group()` |
| `nodes()` | 调度器知道 |
| `PlacementFailed` | 没有东西做 placement |

理由见 [08-project/02-decisions.md](../08-project/02-decisions.md)。
