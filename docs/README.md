# tinyray

> 提案；当前未实现。tinyray 是集群的通用控制面结构层：identity、membership、
> reconciliation 和 discovery。它不分配资源、不拉起进程，也不碰任何 tensor。

| 属性 | 值 |
|---|---|
| 状态 | 提案。取代此前全部 tinyray 设计 |
| 目标规模 | 单实验或共享集群总规模达 10,000 GPU |
| 定位 | [分层](02-architecture/01-layering.md)中的 L2 —— 调度器与应用之间 |
| 对应输入 | [rl-bridge Cell Runtime 提案](../../rl-bridge/docs/08-proposals/02-cell-based-high-availability-runtime.md) |
| 语言 | 正文中文；标识符保留源码拼写 |

## 本文档存在的原因

此前的 tinyray 设计错在无法修补的地方。它假设 tinyray 拉起进程、分配 GPU，并处在
每一条消息的中间。这三条在几百个 worker 之上全部不成立，实测数据见
[01-overview/01-problem.md](01-overview/01-problem.md)。

重新设计把 tinyray 移到唯一没人提供的那一层：每个大型控制面都要手写一遍的机制 ——
带 generation 的逻辑槽位、会过期的 lease、自行收敛的期望状态、不随集群规模增长的
discovery。

## 阅读顺序

目录和文件都带序号，序号即阅读顺序，且没有任何文档依赖排在它后面的文档。

**从这里开始**：[01-overview](01-overview/) 然后 [02-architecture](02-architecture/)。
这两部分是提案主体，约三十分钟。

**如果只是评估边界**，读 [01-overview/02-positioning.md](01-overview/02-positioning.md)
和 [02-architecture/01-layering.md](02-architecture/01-layering.md) 即可，
tinyray 该管什么的完整论证都在这两篇里。

**如果要实现**，[03-modules](03-modules/) 是规格，[04-protocols](04-protocols/) 是
wire 契约。

## 权威位置

同一事实只在一个位置完整定义，其余位置使用链接。

| 信息 | 位置 |
|---|---|
| 设计为何如此 | [02-architecture](02-architecture/) |
| 单个模块的职责与内部状态 | [03-modules](03-modules/) |
| 跨进程消息顺序与 schema | [04-protocols](04-protocols/) |
| 运行与排障 | [05-operations](05-operations/) |
| 测试证据 | [06-testing](06-testing/) |
| 穷举清单：API、配置、指标 | [07-reference](07-reference/) |
| 已建成、已决策、计划中 | [08-project](08-project/) |

## 目录

### 00 规范

- [00-conventions.md](00-conventions.md) —— 文档结构与写作规则

### 01 总览

- [01-problem.md](01-overview/01-problem.md) —— 哪里塌了，附实测数据
- [02-positioning.md](01-overview/02-positioning.md) —— tinyray 处在 L2，以及为何这层最有价值
- [03-principles.md](01-overview/03-principles.md) —— 七条原则，每条都能追溯到一次故障

### 02 架构

- [01-layering.md](02-architecture/01-layering.md) —— L0 到 L4 与归属边界
- [02-topology.md](02-architecture/02-topology.md) —— worker、Cell、global 三层
- [03-state-model.md](02-architecture/03-state-model.md) —— 什么需要共识，什么不需要
- [04-planes.md](02-architecture/04-planes.md) —— 控制面、数据面，以及两者之间的规则

### 03 模块

- [01-identity.md](03-modules/01-identity.md) —— Slot、Incarnation、fencing
- [02-membership.md](03-modules/02-membership.md) —— 分层 lease
- [03-reconciliation.md](03-modules/03-reconciliation.md) —— desired 与 observed 状态
- [04-readiness.md](03-modules/04-readiness.md) —— 可组合 Readiness
- [05-discovery.md](03-modules/05-discovery.md) —— 作用域 lookup
- [06-admission.md](03-modules/06-admission.md) —— backpressure 原语
- [07-transport.md](03-modules/07-transport.md) —— Rust 核心与 GIL 边界
- [08-supervision.md](03-modules/08-supervision.md) —— 节点内进程监督

### 04 协议

- [01-wire-format.md](04-protocols/01-wire-format.md) —— framing
- [02-membership-protocol.md](04-protocols/02-membership-protocol.md) —— register、heartbeat、过期
- [03-control-rpc.md](04-protocols/03-control-rpc.md) —— 调用、结果、错误

### 05 运维

- [01-deployment.md](05-operations/01-deployment.md) —— 部署形态
- [02-failure-model.md](05-operations/02-failure-model.md) —— 故障与恢复矩阵
- [03-observability.md](05-operations/03-observability.md) —— 指标与诊断

### 06 测试

- [01-standard.md](06-testing/01-standard.md) —— 测试标准及其由来
- [02-fake-cluster.md](06-testing/02-fake-cluster.md) —— 10,000 到 100,000 个模拟 worker
- [03-chaos.md](06-testing/03-chaos.md) —— 故障注入矩阵

### 07 参考

- [01-api.md](07-reference/01-api.md) —— Python API
- [02-configuration.md](07-reference/02-configuration.md) —— 全部配置项与默认值

### 08 项目

- [01-status.md](08-project/01-status.md) —— 已有什么，提案什么
- [02-decisions.md](08-project/02-decisions.md) —— 决策与反转
- [03-roadmap.md](08-project/03-roadmap.md) —— 实施阶段

## 提案摘要

**tinyray 负责**：带 generation 和 fencing 的逻辑 identity；分层 lease membership；
desired/observed reconciliation；可组合 Readiness；作用域 discovery；Admission 与
backpressure 原语；控制 RPC transport；节点内进程监督。

**tinyray 不负责**：GPU 与 CPU 分配；拉起作业；任何 tensor；共识存储；以及全部领域概念
—— task、sample、model version、checkpoint 都属于应用。

**核心论断**：万卡控制面的崩溃来自二次复杂度关系和手写存活性判断，不是来自代码慢。
去掉这两者剩下的是一个小而通用、可测试的库 —— 而且可以在预约任何一张 GPU 之前，用
100,000 个 fake worker 验证完毕。
