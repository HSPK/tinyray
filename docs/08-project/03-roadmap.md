# 路线图

> 提案；当前未实现。

> 按每个阶段解锁什么排序。每个阶段独立有价值，都不需要下一个阶段才值得做。

## 阶段 0 —— 基线与压测框架

**为何排在最前。** 本提案中每个数字都是推导或外推。而修正这一点的框架恰恰是最便宜的一个，
因为一个模拟 worker 就是一个没有应用的 worker。

- fake cluster 框架，线程与异步模式
- 带注入时间线记录的 chaos 框架
- 建立“与 worker 数无关”的断言：
  共识写入、lookup 字节、summary 字节、指标基数
- 采集真实分布：控制延迟、抖动率、故障率

**出口条件。** 100,000 个模拟 worker 稳态运行，且那四条指标是被**断言**而不是被绘图。

## 阶段 1 —— Identity 与 fencing

- `Slot`、`Incarnation`、fencing token
- 在 transport 中强制，而不在调用点
- 取代上报与回调
- chaos：旧进程仍存活且仍在写入时重启

**出口条件。** 两个进程都在运行的情况下脑裂被 fence 住。

## 阶段 2 —— 分层 Membership

- Cell 层，worker lease 终止于此
- 固定大小的 Cell summary
- Cell 对共识持有 lease
- Readiness 组合与发布
- 带版本变更检测的作用域 discovery

**出口条件。** 10,000 个模拟 worker 下，共识写入速率保持不变，lookup 大小跟随作用域而非
集群规模。

## 阶段 3 —— Reconciliation

- 基于 etcd 的共识适配层
- 带 fencing token 的 leadership
- desired/observed 收敛循环
- 为需要固定集合的操作提供 membership epoch
- chaos：leader 切换、旧 leader 存活返回

**出口条件。** 可以反复杀掉 leader，Cell 持续运行，且没有过期写入被接受。

## 阶段 4 —— 删除

只在替代品被验证之后进行，因为先删会留下一段两边都不可用的时期。

- placement、资源表、gang placement
- actor launcher 与预热池
- driver 侧 head 与监督循环
- `link()` roster 推送
- worker group 抽象
- collective registry

**推导**：删除约 3,100 行。

**出口条件。** 没有公共 API 接受资源数量，由结构性测试断言。

## 阶段 5 —— 生产加固

- 认证，或一份明确的网络隔离要求
- 推送式 watch
- 环形缓冲之外的日志持久化
- wire format 版本化

## 阶段 6 —— 真实硬件

模拟无法覆盖的一切：

- 端到端多机
- 真实 GPU 设备分配上报
- 一个真实框架：SGLang、vLLM 或 Megatron，未经修改
- Cell 重建下的 NCCL 行为

**出口条件。** 一个真实作业在本控制面下运行，其 worker 脚本只加了一行。

## 不在计划内

| 事项 | 原因 |
|---|---|
| 数据面 | 属于 L0 |
| 资源分配 | 属于 L1 |
| task、sample 或 version 语义 | 属于 L3 |
| 分布式对象存储 | 永久超出范围 |
| collective 的弹性重塑 | 属于框架 |
| 跨集群联邦 | 没有用例 |

## 关于排序的说明

阶段 0 排在阶段 1 之前是承重的选择。先造机制、后做测量，正是此前的设计一直做到 0.2.1
才发现其核心操作是二次复杂度的原因。
