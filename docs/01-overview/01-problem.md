# 问题

> 提案；当前未实现。

> 万卡控制面的崩溃来自二次复杂度关系和手写存活性判断，不是来自代码慢。此前的
> tinyray 设计两样都有，而且都不是能修补的。

## 1. 范围

本文用本仓库的实测数据记录此前的 tinyray 设计为何无法达到目标规模，是
[02-positioning.md](02-positioning.md) 的输入。

以下每个数字都按 [00-conventions.md §9](../00-conventions.md#9-数字) 标注为
**实测**、**推导** 或 **待测**。

## 2. 此前设计的三条假设

三条假设都是在“单机 32 个 actor”这个目标下做出的：

| 假设 | 后果 |
|---|---|
| tinyray 拉起进程 | 需要 launcher、placement 引擎和资源表 |
| tinyray 分配 GPU | 每个 API 带 `num_gpus`，需要设备账本和 gang placement |
| driver 在中心 | 所有消息中转；`link()` 推送完整 roster |

在原定规模下这三条都合理。到万卡全部不成立：作业由 Slurm、Kubernetes 或 `torchrun`
拉起，GPU 在 tinyray 被 import 之前就已分配，而单个 driver 不可能处在一个跨集群运行的
循环的路径上。

## 3. Roster 推送是二次的

`link()` 把每个成员的 endpoint 发给每个成员。

**实测**，用 `tinyray.serde.serialize` 对含 `N` 条记录的双 group roster
（32 位十六进制 actor id、`host:port`、整型 rank）测量：

| N | 每次推送的 roster | 总推送量 |
|---:|---:|---:|
| 128 | 4.2 KB | 0.001 GB |
| 1,024 | 34.0 KB | 0.035 GB |
| 8,192 | 277.7 KB | **2.275 GB** |

推送是 O(N) 次调用，每次携带 O(N) 字节，全部从一个进程发出。8,192 个 worker 时一次
介绍就从 driver 搬走 2.3 GB；10,000 时**推导**为 3.4 GB。

把 driver 写得更快解决不了任何问题。roster 本身的形状就是错的：告诉一个 worker
9,999 个 peer，只为了让它找到需要的那四个。

## 4. 单进程扇出是线性且串行的

**实测**，单机 16 个真实 worker 进程，连接已预热：

| 操作 | 16 worker | 每 worker |
|---|---:|---:|
| `link()` | 3.7 ms | 233 µs |
| `run()` 扇出 | 3.5 ms | 221 µs |

**推导**到 10,000 个 worker：一次介绍 2.3 s，每次广播 2.2 s。作为一次性控制操作勉强能
接受，作为每轮迭代的操作是致命的。

## 5. 没拉起过任何东西的 supervisor 无法判断存活

此前的设计通过监督子进程来检测死亡。当 launcher 是 Slurm 或 Kubernetes 时，tinyray
没有子进程，所以这套机制不是扩展性差 —— 它根本不存在。

替代方案只能是 lease：worker 自己声明存活，缺席即信号。

**实测**，单 registry 副本，单个串行客户端：

| 操作 | 吞吐 |
|---|---:|
| `register` | 4,168 ops/s |
| `heartbeat` | 4,295 ops/s |
| `lookup`（8 个 rank） | 1,933 ops/s |

**推导**：10,000 个 worker 按 10 s heartbeat 是 1,000 ops/s，单副本有 4.3 倍余量。
由于客户端是串行的，服务端并未饱和，这个数字是下界。

## 6. Lease 不能放进共识存储

直觉是把每个 worker 的 lease 放进 etcd。公开数据不支持这个做法：

| 事实 | 来源 |
|---|---|
| Kubernetes 官方支持上限 5,000 节点 | [大集群指南](https://kubernetes.io/docs/setup/best-practices/cluster-large/) |
| 节点 lease 每 10 s 续约，是 etcd 压力的已知来源 | 同上 |
| 每次 lease 续约都走 Raft 提交，受最慢成员磁盘限制 | [etcd 性能](https://etcd.io/docs/v3.4/op-guide/performance/) |
| 标准缓解手段是拉长间隔或拆分 etcd | 同上 |

一万个 worker lease 是一个受支持 Kubernetes 集群全部节点预算的两倍，而且要叠加在该
集群本身的负载之上。因此 lease 必须**分层**：worker 对自己的 Cell 持有 lease，Cell 对
共识存储持有 lease。共识存储看到的是 O(cells) 而不是 O(workers)。见
[02-architecture/02-topology.md](../02-architecture/02-topology.md)。

## 7. 这个规模下故障是连续的

目标集群上的数据**待测**，但公开数据已经足够明确：

| 系统 | 规模 | 可靠性 |
|---|---:|---|
| Llama 3 405B | 16,384 H100 | 54 天内 419 次非计划中断 —— **推导**平均间隔 3.09 小时；78% 为硬件原因 |
| Meta 研究集群 | 16,384 GPU | 预测 MTTF **1.8 小时** |
| MegaScale | 12,288 GPU | 单次生产训练中自动修复超过 100 次 |

来源：[Llama 3](https://arxiv.org/abs/2407.21783)、
[大规模 ML 集群可靠性](https://arxiv.org/abs/2410.21680)、
[MegaScale](https://arxiv.org/abs/2402.15627)。

恢复单元是“整个作业”的控制面，会把大部分生命耗在恢复上。恢复单元必须小于作业，且设计
必须假设任何时刻都有故障正在发生。

## 8. 全局操作的成功率随规模超线性恶化

**推导**，某控制操作重复 `n` 次，每次在 retry 之后的最终成功率为 `p`：

| p | n = 5,000,000 全部成功 |
|---:|---:|
| 99.999% | 1.9e-22 |
| 99.9999% | 0.67% |
| 99.99999% | 60.7% |
| 99.999999% | 95.1% |

任何要求全体成员完成、且每轮迭代重复的设计，都需要没有任何分布式系统能达到的单次可靠性。
结论不是“多重试几次”，而是**任何全局操作都不得要求全体成员**。

## 9. 由此得出的替换

| 失效假设 | 替代方案 |
|---|---|
| 全量 roster 推送 | 作用域 lookup，大小由请求决定 —— [03-modules/05-discovery.md](../03-modules/05-discovery.md) |
| driver 扇出 | 分层；global 层只寻址 Cell —— [02-architecture/02-topology.md](../02-architecture/02-topology.md) |
| 用父子关系判断存活 | Lease —— [03-modules/02-membership.md](../03-modules/02-membership.md) |
| lease 放进共识 | 分层 lease —— [02-architecture/03-state-model.md](../02-architecture/03-state-model.md) |
| 作业级故障单元 | Cell 级 —— [05-operations/02-failure-model.md](../05-operations/02-failure-model.md) |
| 全体成员参与的操作 | 健康 membership 的定额 —— [03-modules/03-reconciliation.md](../03-modules/03-reconciliation.md) |
| tinyray 分配并拉起 | 两者都不做 —— [02-positioning.md](02-positioning.md) |

## 10. 本分析的局限

- 扇出和 registry 吞吐数据都是单机、loopback、单客户端。真实网络增加延迟，真实集群增加
  并发；前者使这些数字偏乐观，后者使其偏悲观。
- 本文没有任何数字来自目标集群。冻结设计前必须测量的清单见
  [06-testing/02-fake-cluster.md](../06-testing/02-fake-cluster.md)。
- 可靠性数据来自预训练作业。RL 额外引入推理引擎和 sandbox，因此上述故障率应视为下界。

## 11. 源码映射

测量针对当前实现进行：

- `python/tinyray/mesh.py` —— 被替换的 roster 推送
- `python/tinyray/registry.py`、`python/tinyray/cluster.py` —— lease 原型
- `benchmarks/` —— 扇出压测脚本
