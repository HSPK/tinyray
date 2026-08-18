# tinyray

**面向「你没有拉起的集群」的控制面结构层。**

> `docs/` 中的设计是**提案**。已发布的包实现的是此前的设计，达不到本提案的目标规模。见
> [docs/08-project/01-status.md](docs/08-project/01-status.md)。

tinyray 提供每个大型控制面最终都要手写一遍的机制：带 generation 的逻辑槽位、会过期的
lease、自行收敛的期望状态、不随集群规模增长的 discovery。

它不分配资源、不拉起进程，也不碰任何 tensor。

```python
# trainer.py —— 由 torchrun、srun 或 Kubernetes Job 拉起
import torch.distributed as dist
import tinyray

dist.init_process_group("nccl")          # 你的
trainer = build_trainer()                # 你的
tinyray.join(trainer, group="trainer")   # 唯一一行 tinyray；立即返回
```

```python
# 控制器 —— 任何地方：登录节点、notebook、另一个 worker
cluster = tinyray.attach()
cluster.group("trainer").wait_ready(size=1024)

# peer —— worker 内部
tinyray.group("ingest").shard(my_dp, num_dp)[0].accept.remote(reference)
```

没有 `num_gpus`、没有 placement、没有 launcher。这些在 tinyray 被 import 之前，调度器就
已经做完了。

## 它在哪一层

| 层 | 归属 |
|---|---|
| 领域：agent、trajectory、reward | 你的应用 |
| 应用控制：task、sample、model version、checkpoint | 你的应用 |
| **控制面机制：identity、membership、reconciliation、discovery** | **tinyray** |
| 资源与进程生命周期 | Slurm、Kubernetes、Volcano、`torchrun` |
| 大数据传输：weight、sample | NCCL、UCX、NIXL、对象存储 |

上面那层是你的产品，下面那些层已经被解决。中间这一层被反复手写 —— 为本工作评审的一份设计
里有 **15 种** identity 类型，每种都需要自己的 generation 与 fencing 校验。

## 为什么重新设计

此前的设计假设 tinyray 拉起进程、分配 GPU，并处在每条消息的中间。本仓库实测：

| 问题 | 实测 |
|---|---|
| roster 推送是二次的 | 8,192 worker 时从一个进程发出 2.3 GB |
| 扇出是串行的 | 每 worker 233 µs —— 一万个就是 2.3 s |
| 靠监督判断存活 | Slurm 拉起作业时根本不可能 |
| 共识里的 per-worker lease | Kubernetes 正是做这件事时到 5,000 节点封顶 |

没有一条能靠把代码写快解决。完整分析见
[docs/01-overview/01-problem.md](docs/01-overview/01-problem.md)。

## 文档

从 [docs/](docs/) 开始。目录与文件按阅读顺序编号，正文中文，术语与标识符保留英文。

| 章节 | 内容 |
|---|---|
| [01-overview](docs/01-overview/) | 哪里塌了、tinyray 在哪一层、七条原则 |
| [02-architecture](docs/02-architecture/) | 分层、拓扑、状态模型、控制面与数据面 |
| [03-modules](docs/03-modules/) | Identity、Membership、Reconciliation、Readiness、Discovery、Admission、Transport、Supervision |
| [04-protocols](docs/04-protocols/) | wire format、membership、控制 RPC |
| [05-operations](docs/05-operations/) | 部署、故障模型、可观测性 |
| [06-testing](docs/06-testing/) | 测试标准、fake cluster、chaos |
| [07-reference](docs/07-reference/) | API、配置 |
| [08-project](docs/08-project/) | 状态、决策、路线图 |

最短的有用路径是三篇：
[定位](docs/01-overview/02-positioning.md) →
[分层](docs/02-architecture/01-layering.md) →
[状态](docs/08-project/01-status.md)。

## 为什么用 Rust

一次测量就决定了。在四个 GIL 绑定的 Python 线程运行时解码 10 MB，原生线程发起是 **1.04
倍**，Python 发起是 **49 倍**。代码相同，差别在于工作开始时谁持有 GIL。

因此服务路径由 tokio 驱动且从不需要 GIL：一个被自身框架压满的 worker 仍能应答控制消息 ——
而那正是你最需要它应答的时候。

## 开发

```bash
export PATH="$HOME/.cargo/bin:$PATH"
python3.11 -m venv .venv
.venv/bin/pip install -e ".[test,dev]"
.venv/bin/maturin develop --release
scripts/test.sh          # 与 CI 完全一致
```

## 测试

测试套件围绕七类各自产生过真实 bug 的失效类别构建 —— 见
[docs/06-testing/01-standard.md](docs/06-testing/01-standard.md)。其中两条最重要：

- **断言代价，而不只是结果。** 一次取回 payload 的 readiness 检查耗时 237 ms 而不是
  0.14 ms，而所有功能测试都通过了。
- **可用性论断需要两个实例并杀掉一个。** 一个共享 identity 的 Registry 通过了全部单副本
  测试，而两个副本是永久损坏的。

```bash
cargo test --workspace --release
.venv/bin/python -m pytest tests/ -q
.venv/bin/python scripts/mutate.py
```

## 目录结构

```
crates/tinyray-core/      wire 协议、framing、标识符
crates/tinyray-runtime/   transport、队列、存储
crates/tinyray-py/        PyO3 绑定（全部 unsafe 集中在 buffers.rs）
python/tinyray/           membership、discovery、readiness、admission、supervision
docs/                     提案
```

## 状态

不是生产软件。它是一份设计，底下有一个可用的 transport，以及一份关于「什么从未运行过」的
诚实清单 —— [docs/08-project/01-status.md](docs/08-project/01-status.md)。
