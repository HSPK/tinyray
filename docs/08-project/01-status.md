# 状态

> 提案；当前未实现。

> 一份诚实清单：今天有什么、本提案改变什么、以及什么从未真正运行过。

## 1. 可从当前实现复用的部分

已验证，原样或近乎原样保留：

| 组件 | 状态 | 证据 |
|---|---|---|
| wire framing、上限、带外 frame | 完成 | 117 个 Rust 测试 |
| 原生服务路径，不需要 GIL | 完成 | **实测**争用下 1.04 倍 vs 49 倍 |
| per-caller 顺序、重复吸收 | 完成 | 单测覆盖 |
| 带显式拒绝的 Admission | 完成 | 单测覆盖 |
| 结果存储：水位、TTL、墓碑 | 完成 | 单测覆盖 |
| 按 peer 的字节统计 | 完成 | `tests/test_driver_byte_budget.py` |
| 进程组监督与清理 | 完成 | 集成测试覆盖 |
| Readiness 观察谓词 | 完成 | 集成测试覆盖 |
| 带 lease 的 Registry 原型 | 可用 | **实测** 4,295 heartbeat/s |
| 多副本失败转移与缓存回落 | 可用 | chaos 测试覆盖 |
| 测试标准、变异框架 | 完成 | 21/21 变异体被捕获 |

## 2. 待建设的部分

| 组件 | 提案 | 依赖 |
|---|---|---|
| `Slot` / `Incarnation` / fencing | [03-modules/01-identity.md](../03-modules/01-identity.md) | —— |
| transport 中的 fencing 强制 | 同上 | Identity |
| 分层 membership、Cell 层 | [03-modules/02-membership.md](../03-modules/02-membership.md) | Identity |
| Cell summary 聚合 | 同上 | Membership |
| Reconciler 与 leadership 适配层 | [03-modules/03-reconciliation.md](../03-modules/03-reconciliation.md) | 共识适配层 |
| 基于 etcd 的共识适配层 | [02-architecture/03-state-model.md](../02-architecture/03-state-model.md) | —— |
| Readiness 组合与发布 | [03-modules/04-readiness.md](../03-modules/04-readiness.md) | Membership |
| 带版本 watch 的作用域 discovery | [03-modules/05-discovery.md](../03-modules/05-discovery.md) | Membership |
| fake cluster 压测框架 | [06-testing/02-fake-cluster.md](../06-testing/02-fake-cluster.md) | Membership |
| chaos 框架 | [06-testing/03-chaos.md](../06-testing/03-chaos.md) | —— |

## 3. 待删除的部分

每一项删除的都是分层判定 tinyray 不该拥有的能力。

| 删除 | 约行数 | 原因 |
|---|---:|---|
| placement 与资源表 | 800 Rust | 调度器在 tinyray 被 import 前就已分配 |
| gang placement | 200 Rust | tinyray 什么都不放置 |
| actor launcher | 300 Python | launcher 拉起作业 |
| driver 侧 head 与监督循环 | 700 Python | 没有东西监督它没拉起的进程 |
| `link()` roster 推送 | 200 Python | **实测** 8,192 worker 时 2.3 GB |
| worker group 抽象 | 200 Python | `torchrun` 拥有这件事 |
| 预热池 | 150 Python | 与 tinyray 拉起进程绑定 |
| collective registry | 550 Rust | 从未在 GPU 上跑过；epoch 概念迁入 Reconciler |

**推导**合计：删除约 3,100 行，新增约 1,500 行。

## 4. 从未针对真实目标运行过的部分

最重要的一节。以下全部已设计，但从未针对其目标测试过。

| 论断 | 实际验证过的 |
|---|---|
| **一万 worker 规模** | 只到 16 个真实 worker。本文档中每个大数字都是推导或外推 |
| **分层 membership** | 只有单层原型 |
| **共识适配层** | 尚未编写；从未联系过任何 etcd |
| **Cell 控制器切换** | 已设计；未建成 |
| **NCCL 交互** | 从未在 GPU 上运行。此前的 collective 代码是准入规则与状态机，针对 gloo 测试 |
| **真实框架** | SGLang、vLLM 和 Megatron 从未被拉起。使用了启动形状相同的替身脚本 |
| **多机** | 从未端到端运行。跨节点 placement 有单测；从未涉及第二台机器 |

fake cluster 的存在，就是为了在预约任何 GPU 之前把前三行移出这张表。

## 5. 提案自身的已知缺口

记录下来，而不是以后被发现：

| 缺口 | 影响 |
|---|---|
| 没有认证或加密 | 没有网络隔离的共享集群不适用 |
| 没有推送式 watch | 变更延迟为一个轮询周期 |
| Admission 界限是个数而非字节或时间 | 一千个廉价调用和一千个昂贵调用占同样空间 |
| 没有按生产方的公平性 | 一个吵闹的生产方可以吃掉全部配额 |
| 没有日志持久化 | 每进程 200 行环形缓冲；进程死后就剩这些 |
| wire format 无版本化 | 一个集群内混用发布版本不被支持 |
| Cell 大小没有默认值 | 一个运维决定，除作用力之外没有指引 |

## 6. 适用性

**适合**：由 Slurm、Kubernetes 或 `torchrun` 拉起的集群，需要在数千进程之间做 membership、
discovery、fencing 和 Readiness，且应用拥有自己的语义。

**不适合**：希望被代为放置和拉起的作业；需要数据面的场景；有不可信租户的共享集群；以及
今天就需要一个受支持系统而不是一份设计的人。

## 7. 版本

当前发布的包实现的是此前的设计。本提案尚未实现。迁移路径见
[03-roadmap.md](03-roadmap.md)。
