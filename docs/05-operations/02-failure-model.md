# 故障模型

> 提案；当前未实现。

> 假设总有故障正在发生。在目标规模上，确实总有。

## 1. 问题

这个规模集群的公开数据：

| 系统 | 规模 | 可靠性 |
|---|---:|---|
| Llama 3 405B | 16,384 H100 | 54 天内 419 次非计划中断 —— **推导**平均间隔 3.09 小时 |
| Meta 研究集群 | 16,384 GPU | 预测 MTTF **1.8 小时** |
| MegaScale | 12,288 GPU | 单次运行中自动修复超过 100 次 |

来源：[Llama 3](https://arxiv.org/abs/2407.21783)、
[大规模 ML 集群可靠性](https://arxiv.org/abs/2410.21680)、
[MegaScale](https://arxiv.org/abs/2402.15627)。

恢复单元是整个作业的控制面，会把大部分生命耗在恢复上。

## 2. 目标

- 每个故障都有检测方、时限和明确的爆炸半径。
- 没有单一故障能停掉作业。
- 降容量而不是降可用性。

## 3. 非目标

- 恢复应用工作。tinyray 上报，L3 决策。
- 预防硬件故障。
- 任何 exactly-once 语义。

## 4. 设计

### 4.1 检测

| 对象 | 检测方 | 时限 |
|---|---|---|
| worker 死亡或挂起 | 其 Cell 的 lease 过期 | `lease_ttl + sweep` 约 35 s |
| worker 存活但未就绪 | Readiness 谓词 | readiness 周期，约 1 s |
| 被监督进程退出 | Node Agent 轮询 | 约 1 s |
| Cell 死亡 | global 处 cell lease 过期 | `cell_ttl` 约 15 s |
| leader 丢失 | 共识 lease | `leader_ttl` 约 15 s |
| 过期写入者 | 接收路径上的 fencing | 立即 |
| 过载 | Admission 深度 | 立即 |

Readiness 比 membership 快，这是有意的：一个存活但没用的 worker，应该在任何人断定它已死
之前很久就停止接活。

### 4.2 爆炸半径

| 故障 | 半径 | 作业影响 |
|---|---|---|
| worker | 1 个 worker | 容量减一 |
| 被监督进程 | 1 个进程 | 其 worker 转为未就绪 |
| 节点 | 1 个节点 | 其 worker 过期 |
| Cell Registry | 该 Cell 内的 lookup | 缓存服务；观察不到 membership 变更 |
| Cell 控制器 | 该 Cell 内的调度 | 运行中的工作继续 |
| Cell | 1 个 Cell | **推导** 128 GPU Cell 时为 2.56% 的 rollout 容量 |
| global leader | 新决策 | Cell 继续 |
| 共识 | leadership 与配置 | 其余一切继续 |

没有一行是“整个作业”。

### 4.3 使之成立的规则

> 任何操作都不得要求全体成员。

**推导**：即使单次可靠性 99.9999%，五百万次控制操作全部成功的概率只有 0.67%。完整表格见
[01-overview/01-problem.md §8](../01-overview/01-problem.md#8-全局操作的成功率随规模超线性恶化)。

当某操作确实需要一个固定集合时 —— 例如 collective communicator —— 把 membership 冻结成
一个 epoch，该操作要求**该 epoch 内**的全部成员。返回的成员加入下一个。

### 4.4 分区行为

| 分区 | 行为 |
|---|---|
| worker 与 Cell | lease 过期；worker 重连后重新注册 |
| 节点与 Cell | node lease 过期；其进程被回收 |
| Cell 与 global | Cell 完成有效工作，不申请新的；lease 过期后停止 |
| global 与共识 | 无 leadership 与配置变更；现有状态维持 |
| 读取方与全部副本 | lookup 走缓存，并上报陈旧 |

从任何分区恢复都要经过 fencing。没有任何东西自动恢复其此前的写权限。

## 5. 正常流程

```mermaid
sequenceDiagram
    participant W as worker
    participant C as Cell
    participant G as global
    participant A as 应用

    Note over W: 死亡
    C->>C: lease 过期
    C->>G: summary 显示容量下降
    G->>A: observed state 变化
    A->>A: 决策（重新分配、缩容、等待）
    ```

图中无法表达：最后一步 tinyray 不做任何决策；Cell 在一个 TTL 内察觉，而 global 在一个
Cell 周期内得知；没有任何组件尝试重启任何东西。

## 6. 状态与所有权

故障状态是软状态。tinyray 中除指标与近期输出外没有故障日志；需要故障历史的应用自己保存。

## 7. 正确性不变量

- 每个故障模式都有指名的检测方和时限。
- 没有检测方从父子进程关系推断存活性。
- 分区恢复经过 fencing。
- 降级的组件上报降级，而不是静默失败。
- 一个 Cell 的故障不改变另一个 Cell 的 membership 或 communicator。

## 8. 故障与恢复

| 故障 | 恢复 | 是否自动 |
|---|---|---|
| worker 死亡 | 过期；重启后重新注册 | 检测是，重启否 |
| worker 挂起 | 过期 | 是 |
| 被监督进程退出 | 上报 | 检测是，重启否 |
| 节点丢失 | 其 worker 过期 | 是 |
| Registry 副本丢失 | 失败转移；补齐 | 是 |
| 全部副本丢失 | 缓存；重新填充 | 是 |
| Cell 控制器丢失 | standby 以新 generation 接管 | 是 |
| global leader 丢失 | 选举 | 是 |
| 共识丢失 | 从备份恢复 | 否 |
| 过期进程返回 | 被 fence 出去 | 是 |

**tinyray 不重启任何它没有拉起的东西**，而且即使是它拉起的，也不重启 collective 的成员。
在不重建 communicator 的情况下重启一个 rank，会让其余 rank 永久阻塞 —— 那是挂起，不是
报错。

## 9. 可观测性

| Metric | 用途 |
|---|---|
| `membership_evictions_total` | 故障率 |
| `fencing_rejections_total` | 脑裂，重启期间属预期 |
| `registry_served_from_stale` | Registry 可达性 |
| `leader_changes_total` | 控制面抖动 |
| `cell_ready_capacity` | 按 Cell 的降级情况 |
| `readiness_failures_by_reason` | worker 为何不可用 |

## 10. 取舍

- **检测不是即时的。** 有界且被上报；缩短 TTL 会在停顿期间驱逐健康 worker。
- **tinyray 从不自愈。** 它检测并上报。每一种恢复策略都需要 tinyray 拒绝拥有的应用知识。
- **Cell 控制器是调度的局部单点**，但不是运行中工作的单点。
- **Cell 大小是运维决定**，并决定爆炸半径。

## 11. 实现与测试

§4.2 的每一行都有一个 chaos 用例 ——
[06-testing/03-chaos.md](../06-testing/03-chaos.md)。**没有注入测试的故障模式是假设，
不是设计。**
