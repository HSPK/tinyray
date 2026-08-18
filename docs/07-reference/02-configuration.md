# 配置

> 提案；当前未实现。

每个值都是由设计**推导**得出，或是每次部署**待测**。在 fake cluster 运行之前，没有一个是
生产默认值（[06-testing/02-fake-cluster.md](../06-testing/02-fake-cluster.md)）。

## 1. Membership

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `lease_ttl` | s | 30 | > 3 × heartbeat | 到驱逐的时间 |
| `heartbeat_interval` | s | `ttl/3` | > 0 | 声明频率 |
| `sweep_interval` | s | 5 | > 0 | 驱逐粒度 |
| `startup_window` | s | 300 | > 0 | 等待尚未启动的 Registry |
| `registry` | 地址 | `TINYRAY_REGISTRY` | 非空 | 副本列表，逗号分隔 |

`lease_ttl` 必须超过一次合理的 GC 停顿。太短会驱逐健康 worker，太长会让已死地址留在
lookup 里。

## 2. Discovery

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `cache_ttl` | s | 5.0 | >= 0 | lookup 新鲜度 |
| `watch_interval` | s | 2.0 | > 0 | 变更检测延迟 |
| `lookup_timeout` | s | 10.0 | > 0 | 每副本 |
| `max_scope` | int | 1024 | > 0 | 拒绝过大的 lookup |

## 3. Readiness

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `interval` | s | 1.0 | > 0 | 求值频率 |
| `predicate_timeout` | s | 1.0 | > 0 | 每谓词 |
| `verdict_max_age` | s | 3 × interval | > interval | 陈旧裁决视为未就绪 |

初始状态是未就绪，且不可配置。

## 4. Admission

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `max_pending` | int | 1000 | > 0 | 界限 |
| `high_watermark` | 比例 | 0.8 | 0..1 | 进入 pressured |
| `low_watermark` | 比例 | 0.6 | < high | 离开 pressured |
| `retry_after_base` | s | 0.025 | > 0 | 线性退避步长 |
| `retry_after_max` | s | 1.0 | > base | 上限 |
| `max_retries` | int | 16 | >= 0 | backpressure retry 次数 |

## 5. Reconciliation

| Field | Type | Default | Validation | Effect |
|---|---|---|---|---|
| `interval` | s | 2.0 | > 0 | 收敛频率 |
| `leader_ttl` | s | 15 | > 3 × renew | 切换窗口 |
| `leader_renew` | s | `ttl/3` | > 0 | 续约频率 |
| `min_ready_fraction` | 比例 | 0.9 | 0..1 | 低于此值拒绝冻结 epoch |
| `consensus` | 地址 | `TINYRAY_CONSENSUS` | 使用时非空 | 存储位置 |

## 6. Transport

| Field | Type | Default | Effect |
|---|---|---|---|
| `connections_per_peer` | int | 4 | 缓解队头阻塞 |
| `request_timeout` | s | 300 | 每请求 |
| `max_pending_calls` | int | 1000 | 服务端 Admission 界限 |
| `max_header_len` | 字节 | 1 MiB | 分配护栏 |
| `max_frames` | int | 4096 | 分配护栏 |
| `max_frame_len` | 字节 | 4 GiB | 分配护栏 |
| `max_message_len` | 字节 | 8 GiB | 分配护栏 |
| `result_ttl` | s | 300 | 未被取回的结果 |

## 7. Supervision

| Field | Type | Default | Effect |
|---|---|---|---|
| `ready_when` | 谓词 | `alive` | 如何观察 readiness |
| `startup_timeout` | s | 600 | 模型加载很慢 |
| `stop_timeout` | s | 30 | kill 整组前的宽限 |
| `log_lines` | int | 200 | 环形缓冲深度 |

`ready_when="alive"` 几乎什么都证明不了。任何提供服务的东西都应使用 `port`、`http` 或
`log:`。

## 8. 环境变量

| 变量 | 读取方 | tinyray 是否写入 |
|---|---|---|
| `TINYRAY_REGISTRY` | 客户端 | 否 |
| `TINYRAY_CONSENSUS` | Reconciler | 否 |
| `TINYRAY_CONTROL_PORT` | `join` | 仅对它监督的进程写入 |
| `RANK`、`SLURM_PROCID`、`OMPI_COMM_WORLD_RANK` | `join` | **绝不** |
| `WORLD_SIZE`、`SLURM_NTASKS` | `join` | **绝不** |
| `LOCAL_RANK`、`SLURM_LOCALID` | `join` | **绝不** |
| `CUDA_VISIBLE_DEVICES` | `join`，记入 meta | **绝不** |

“绝不”那几行由 `tests/test_suite_quality.py` 断言。写入它们会让集群对“我的 rank 是多少”
有两个答案。

## 9. 时间汇总

由上述默认值**推导**：

| 事件 | 时限 |
|---|---|
| Cell 处检测到 worker 死亡 | `lease_ttl + sweep` 约 35 s |
| 检测到 worker 未就绪 | readiness 周期，约 1 s |
| global 处检测到 Cell 死亡 | `cell_ttl` 约 15 s |
| leader 切换 | `leader_ttl` 约 15 s |
| membership 变更对读取方可见 | `lease + cache_ttl` 约 35 s |
| 收敛延迟 | `interval` 约 2 s |

每个时间常量都可由环境变量覆盖，使测试能在秒级触达 deadline。只在生产值上运行过的常量，
是一个没人测过的常量。
