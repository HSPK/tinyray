# 设计原则

> 提案；当前未实现。

> 七条原则。每条都是在某件事出问题之后写下的，且都写成可以在评审中直接引用的形式。

## 1. 范围

这些原则约束 `03-modules/` 和 `04-protocols/` 中的每一个设计决定。模块若违反某条原则，
必须在“限制与取舍”中说明违反了哪条以及为什么。

## 2. 原则

### P1 —— 控制面绝不搬运大数据

控制消息是 KB 级。更大的属于 L0。

**由来。** `wait()` 曾通过取回 payload 再丢弃来回答一个 readiness 问题。**实测**：
一个已就绪的 200 MB 结果耗时 237 ms，改为只问状态后为 0.14 ms。所有功能测试都通过，
因为答案是对的。

**强制手段。** 每个控制操作都有由测试断言的字节预算，并有一条元测试要求每个接触 wire
的操作都必须有预算。见 [06-testing/01-standard.md](../06-testing/01-standard.md)。

### P2 —— 绝不占用进程已经拥有的资源

tinyray 不分配 GPU，不占用默认 process group，不设置 `CUDA_VISIBLE_DEVICES`，不决定
任何东西在哪运行。它读取 launcher 分配的结果并上报。

**由来。** 早期的 collective 模块占用了默认 process group，导致 Megatron 无法初始化
自己的。

**后果。** `num_gpus`、`cpus_per_worker` 和 placement 完全离开 API。一份资源两本账，
就是多了一本。

### P3 —— 对外呈现 launcher 的接口，不自创一套

rank、world size 和 local rank 来自 `RANK`、`SLURM_PROCID`、
`OMPI_COMM_WORLD_RANK` —— launcher 设了什么就用什么。tinyray 不增加自己的编号体系。

**由来。** 任何框架集成的第一个问题都是“我的 rank 是多少”，而这个问题有第二个答案的
地方，就是集成崩掉的地方。

### P4 —— 每个 identity 都带 generation，接收端 fencing

逻辑名标识一个 Slot，Incarnation 标识当前占据它的进程。每次跨进程写入都携带自己的
Incarnation，接收端拒绝过期的。

**由来。** 没有它，重启后的 rank 和它的前身会同时 heartbeat 同一个 lease，交替把一个
已死的地址复活。

**强制手段。** fencing 由 transport 施加，不由各调用点施加。一个需要十五个调用点记得的
机制，是一个会被其中十四个忘掉的机制。

### P5 —— 任何操作都不得要求全体成员

全局操作作用于冻结 epoch 下的健康 membership，并带显式最小值。一个不可达的 worker 不得
阻塞其余全部。

**由来。** **推导**：即使单次成功率 99.9999%，五百万次控制操作全部成功的概率只有
0.67%。完整算式见
[01-problem.md §8](01-problem.md#8-全局操作的成功率随规模超线性恶化)。

**后果。** 降容量，不停机。98% 是结果，0% 是事故。

### P6 —— 故障必须显式且有界，绝不挂起

每次等待都有 deadline。每个失败都写明期待什么、来自谁、等了多久。挂起的分布式作业不提供
任何信息。

**由来。** 三次各自独立的死锁，形状完全相同：组操作在向其余成员派发之前先等待了某一个
成员。以及反复出现的：对含 collective 的方法只在一个 rank 上调用。

**后果。** 组操作在等待任何成员之前先向全部成员派发。这条适用于**启动**一个组，而不只是
调用一个组。

### P7 —— 优先软状态，只在无法重建处使用共识

由 owner 定时重新声明的状态不需要共识：丢光一切的副本在一个 lease 之后就恢复正确。共识
保留给没有 owner 可以重新声明的状态 —— leadership、desired 配置、分区所有权。

**由来。** **实测**：杀掉全部 Registry 副本后，worker 仍从缓存互相寻址，训练不受影响。
一个陈旧的 endpoint 远比一个停掉的作业有价值。

**后果。** membership 的复制不需要日志、不需要 leader、不需要达成一致。完整拆分见
[02-architecture/03-state-model.md](../02-architecture/03-state-model.md)。

## 3. 原则之间的冲突

原则在两处冲突，解法在此固定。

**P5 与严格同步。** 有些操作确实需要每个参与者 —— 例如建立 NCCL communicator。解法：
P5 约束的是 **membership**，不是 **collective**。把健康 membership 冻结成一个 epoch，
要求该 epoch 内的全部成员，并把其余成员 fence 出去。返回的成员加入下一个 epoch。
tinyray 提供 epoch，collective 属于框架。

**P7 与 P4。** fencing token 必须单调，而软状态存储无法在全量丢失后保证单调。解法：
Incarnation 派生自可存活的来源 —— Cell 级 identity 用共识计数器，worker 级 identity 用
无需协调即可保证 per-slot 单调的值（见
[03-modules/01-identity.md](../03-modules/01-identity.md)）。worker 的 Incarnation 从不
需要全局唯一，只需要在自己的 Slot 内有序。

## 4. 这些原则排除了什么

写下来以防设计漂回去：

- 处在每轮迭代路径上的 driver。
- 大小随集群增长的 roster。
- 由父子进程关系推断的存活性。
- 共识存储里的 per-worker lease。
- 任何接受资源数量的 API。
- 任何成功需要全体成员的操作。
- 对非幂等操作的 retry。

## 5. 限制与取舍

- **P7 允许陈旧读。** worker 可能寻址一个已经迁移的 endpoint。fencing 使其安全，但不
  免费：调用会失败并需要重新 lookup 后重试。
- **P5 允许部分完成。** 一次广播可能只到达 98% 的成员。应用必须能说明这意味着什么，
  tinyray 无法替它说明。
- **P2 放弃了防止冲突的能力。** 没有账本，tinyray 无法阻止两个进程占用同一张 GPU。这项
  保护移交给调度器 —— 它本来就该在那里，而且在那里更强。

## 6. 源码映射

每条原则由 `tests/test_suite_quality.py` 中的一个结构性测试断言，对应关系见
[06-testing/01-standard.md](../06-testing/01-standard.md)。
