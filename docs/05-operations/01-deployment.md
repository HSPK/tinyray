# 部署

> 提案；当前未实现。

> 同一份 worker 代码在所有规模上运行。层是被折叠的，绝不被移除。

## 1. 问题

开发在笔记本上，生产在一万张 GPU 上。如果这两者是不同的代码路径，那被测过的就不是被部署
的那个。

## 2. 目标

- 从笔记本到集群，`join()` 调用完全一致。
- 资源分配的关键路径上没有任何 tinyray 组件。
- 每一层都可独立重启。

## 3. 非目标

- 打包、镜像或调度器配置。
- 选定共识存储。

## 4. 设计

### 4.1 形态

| 形态 | global | Cell | Node Agent | 共识 |
|---|---|---|---|---|
| 笔记本 | 进程内 | 1 个进程内 | 0 | 无 |
| 单节点 | 进程内 | 1 | 1 | 无 |
| 小集群 | 1 副本 | 每节点 1 | 每节点 1 | 可选 |
| 生产 | 3 或 5 副本 | 每故障域 1 | 每节点 1 | 必需 |

被折叠的层仍以对象形式存在，因此代码路径一致，变的只有地址。

### 4.2 启动顺序

不要求任何特定顺序，这正是重点。

```mermaid
flowchart LR
    A[共识] --> B[global 副本]
    B --> C[Cell Registry]
    C --> D[worker]
    D -.在 startup_window 内重试.-> C
    C -.重试.-> B
```

早于自己 Registry 启动的 worker 会重试 `startup_window`（默认 300 s）而不是失败。调度器
以任意顺序拉起 rank，一个因为启动太早而放弃的 worker 会把启动变成竞态。

### 4.3 调度器做什么

关于资源的一切。tinyray 读取结果：

| 变量 | 用于读取 |
|---|---|
| `RANK`、`SLURM_PROCID`、`OMPI_COMM_WORLD_RANK` | rank |
| `WORLD_SIZE`、`SLURM_NTASKS` | world size |
| `LOCAL_RANK`、`SLURM_LOCALID` | local rank |
| `CUDA_VISIBLE_DEVICES` | 记入 `meta`，从不写入 |
| `HOSTNAME` | 记入 `meta` |

tinyray 一个都不写。

### 4.4 一个 worker

```python
import tinyray
import torch.distributed as dist

dist.init_process_group("nccl")     # 你的
trainer = build_trainer()           # 你的
tinyray.join(trainer, group="trainer")   # 立即返回
```

由 `torchrun`、`srun` 或 Kubernetes Job 拉起，拉起方式不做任何改动。

### 4.5 一个 Registry 副本

```bash
tinyray registry --bind 0.0.0.0:7777 --ttl 30
```

无状态。每个 Cell 跑两到三个。它们之间不通信。

## 5. 正常流程

共识启动，global 副本选出 leader，Cell Registry 启动，worker 注册。任何组件都可以早于或
晚于任何其他组件启动。

## 6. 状态与所有权

见 [02-architecture/03-state-model.md](../02-architecture/03-state-model.md)。只有共识存储
需要备份。

## 7. 正确性不变量

- 没有组件要求另一个必须先启动。
- 没有 tinyray 组件写入 launcher 的环境变量。
- 除共识外，重启任一单个组件都不会丢失其 owner 无法重新声明的状态。

## 8. 故障与恢复

| 重启的组件 | 恢复 | 作业影响 |
|---|---|---|
| Registry 副本 | 一个 heartbeat 间隔内重新填充 | 无 |
| 全部 Registry 副本 | 重启后重新填充 | 期间 lookup 走缓存 |
| Cell Registry | 其 worker 重新注册 | Cell 短暂不再调度 |
| global 副本 | 若它是 leader 则重新选举 | 短暂不做配置变更 |
| 共识 | 从备份恢复 | 无 leadership 与配置变更 |
| worker | 以新 Incarnation 重新注册 | 其工作属于 L3 关心的范围 |

## 9. 可观测性

每个组件都提供纯 JSON 的 `/health` 和 `/introspect`，因此不需要 tinyray 客户端，`curl`
就够。见 [03-observability.md](03-observability.md)。

## 10. 取舍

- **要运维两个存储**，其中一个需要备份。
- **对打包不持观点。** tinyray 提供进程，不提供镜像或 chart。
- **生产规模以下共识是可选的**，这意味着较小形态不会演练 leadership。这个缺口由 fake
  cluster 覆盖 —— [06-testing/02-fake-cluster.md](../06-testing/02-fake-cluster.md)。

## 11. 实现与测试

| Behavior | Test file |
|---|---|
| 四种形态运行同一份 worker 代码 | `tests/test_deployment_shapes.py` |
| 早于 Registry 启动的 worker 能成功 | `tests/test_membership.py` |
| 没有组件写入 launcher 变量 | `tests/test_suite_quality.py` |
| 每个组件无需客户端即可应答 `/health` | `tests/test_observability.py` |
