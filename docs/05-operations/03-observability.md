# 可观测性

> 提案；当前未实现。

> 分布式 ML 排障中占主导地位的只有一个问题：哪个 worker 卡住了，卡在什么上。这里的一切都
> 是为了回答它。

## 1. 问题

一万个 worker 的规模下，聚合指标会掩盖故障。一个 Cell 已经死了而平均延迟依然健康；一个
已经不再前进的作业，和一个只是慢的作业看起来一模一样。

## 2. 目标

- 用一条命令回答“哪个 worker 卡住了，卡在什么上”。
- 上报 worker 自己的判断，而不是控制器的猜测。
- 让 global 层的指标基数与 worker 数无关。

## 3. 非目标

- 存储指标。tinyray 暴露，TSDB 存储。
- 聚合其他层的指标。
- 日志存储。输出异步进入对象存储，不经过任何控制器。

## 4. 设计

### 4.1 Endpoint

| 路径 | 内容 | 格式 |
|---|---|---|
| `/health` | 存活、身份、是否 draining | JSON |
| `/introspect` | 队列、readiness 原因、inflight 方法与时长、Admission 深度、Incarnation | JSON |
| `/metrics` | 计数器与量表 | Prometheus |

`/health` 和 `/introspect` 是纯 JSON，因此不需要 tinyray 客户端，`curl` 就能用。它们由
原生 transport 服务，所以一个 Python 已经卡住的 worker 仍然会应答 —— 而那正是最需要答案的
时刻。

### 4.2 分层归约

| 层 | 基数 | 上报 |
|---|---|---|
| worker | 每 worker，本地抓取 | Readiness、队列、inflight、Admission |
| Cell | 每 Cell | ready 容量、驱逐数、抖动、控制延迟 |
| global | 每集群 | 存活 Cell、不可用容量、leader 变更、共识写入速率 |

per-worker 时间序列绝不上到 global 层。一万个 worker 乘十几条序列，在监控系统里就是基数
问题 —— 然后它会和集群同时挂掉。

### 4.3 诊断命令

```
tinyray status <cell-或-endpoint>...
```

每个 worker 一行，然后列出所有看起来不对的：

```
ENDPOINT              READY  INFLIGHT        SECS  QUEUED  DEPTH  INCARNATION
10.0.3.7:41234        yes    train_step       0.5       0     12  @1739..a1
10.0.3.8:41234        no     -                0.0       0      0  @1739..b2

Problems:
  - 10.0.3.8 未就绪：model_version_in_window 失败
  - 10.0.4.1 正在等待调用方 3f2a 的 seq 7，其后已缓冲 4 个（有一次调用丢失）
  - 10.0.4.2 因 backpressure 拒绝了 340 次调用；它比它的调用方慢
  - 10.0.5.9 已在 train_step 中 94.2s，中位数为 12.1s：疑似掉队者
```

无问题时退出码 0，发现问题时 1。掉队者检测至少需要三个运行中的 worker，才有一个值得比较的
中位数。

### 4.4 指标分组

| 组 | 关键序列 |
|---|---|
| Membership | `membership_live`、`membership_evictions_total`、`membership_version` |
| Identity | `fencing_rejections_total`、`identity_superseded_total` |
| Readiness | `readiness_current`、`readiness_failures_by_reason`、`readiness_transitions_total` |
| Discovery | `discovery_response_bytes`、`discovery_served_from_stale_total` |
| Admission | `admission_depth`、`admission_rejections_total`、`admission_pressured_seconds` |
| Transport | `control_bytes_sent`、`control_retries_total`、`queue_waiting_for` |
| Reconciliation | `reconcile_iterations_total`、`leader_changes_total`、`epoch_current` |
| 共识 | `consensus_writes_total` |

### 4.5 最重要的四条

| 序列 | 原因 |
|---|---|
| `control_bytes_*` 随工作负载增长 | 有 payload 进入了控制面 |
| `consensus_writes_total` 随 worker 数增长 | 状态拆分被违反了 |
| `discovery_response_bytes` 随集群规模增长 | 作用域机制被绕过了 |
| `queue_waiting_for` 非空 | 有调用方被永久卡住 |

每一条都是一个用指标表达的设计不变量，因此回退在生产中可见，而不只在测试中可见。

## 5. 正常流程

在本地抓取 worker，在 Cell 归约，在 global 暴露集群级序列。诊断方向相反：global 说是哪个
Cell，Cell 说是哪个 worker，worker 说是哪个方法。

## 6. 状态与所有权

全部可观测性状态都是软状态且进程本地。tinyray 不持久化任何东西。

## 7. 正确性不变量

- worker 的 Python 阻塞时，`/health` 和 `/introspect` 仍然应答。
- worker 上报自己的判断；没有哪一层替下层编造裁决。
- global 层基数与 worker 数无关。
- 否定的 readiness 裁决必定携带原因。
- 日志绝不经过控制器。

## 8. 故障与恢复

| 故障 | 影响 |
|---|---|
| 抓取失败 | 序列变陈旧；worker 不受影响 |
| TSDB 宕机 | 没有历史；集群不受影响 |
| `/introspect` 不可达 | 上报为 `UNREACHABLE`，与不健康区分 |

## 9. 可观测性自身的可观测性

`tinyray status` 会列出哪些 endpoint 没有应答，而不是把它们省略。缺失的一行比一行报错更糟。

## 10. 取舍

- **不存储。** 没有 TSDB 的部署只保留 `tinyray status` 当下显示的内容。
- **CLI 内没有集群级发现。** `status` 接受 endpoint 或一个 Cell。获取它们需要一次 lookup。
- **掉队者检测需要三个 worker**，且是启发式的。
- **缺少日志持久化。** 每进程 200 行环形缓冲，进程死后剩下的就是它。在
  [roadmap](../08-project/03-roadmap.md) 上。

## 11. 实现与测试

| Behavior | Test file |
|---|---|
| Python 阻塞时 `/introspect` 仍应答 | `tests/test_observability.py` |
| global 基数不随 worker 数变化 | `tests/test_fake_cluster.py` |
| 有问题时 `status` 退出码非零 | `tests/test_observability.py` |
| 停滞的队列出现在 `status` 中 | `tests/test_transport.py` |
| 每个否定裁决都有原因 | `tests/test_readiness.py` |
