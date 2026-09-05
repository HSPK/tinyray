# tinyray

*[English](README.md)*

**一本通讯录，加一次点名。** 给异步 ML 作业用的成员关系层。

它不启动进程、不分配 GPU、不搬运 tensor。只回答三个问题：
**谁在？还活着吗？我该找谁？**

```python
import tinyray

me = tinyray.join("engine", "serving")
me.ready(model_version=17)

engine = tinyray.pool("engine").pick(model_version=17)
print(engine.url)
```

## 它做什么

报到、租约、本地缓存、找人，以及调用层：

```python
class Collector:
    def assign(self, task: str) -> dict:
        return {"took": task}

me = tinyray.join("collector", "stateful", slot=0, serves=Collector())
me.ready()

# 另一个进程里
tinyray.pool("collector").slot(0).assign("task-7")
await tinyray.apool("collector").slot(0).assign("task-7")
```

底下就是普通 HTTP，所以 `curl` 排障能力一点没丢：

```bash
curl -X POST http://host:port/call/assign -d '{"task":"t"}'
curl http://host:port/_methods
```

座位、任期与冻结名单：

```python
me = tinyray.join("trainer", "collective", slot=RANK, size=WORLD_SIZE)
me.ready()

ep = tinyray.pool("trainer").epoch()   # 等人齐，然后冻住
build_process_group(ep.members)        # 每个 rank 拿到的必然一样

def watchdog():                        # 训练循环里查是没用的：卡住的 rank 到不了
    while ep.valid:                    # 那一行。后台线程可以，因为 NCCL 阻塞时
        time.sleep(0.5)                # 会放开 GIL。
    pg._abort()
```

## 文档

**<https://hspk.github.io/tinyray/>**

| 文档 | 内容 |
|---|---|
| [上手](docs/getting-started.md) | 十分钟，从装上到两个进程互相调用 |
| [API 参考](docs/api.md) | 完整接口，对着实现写的 |
| [基准](docs/bench.md) | 它花多少钱，量出来的，以及量它时的坑 |
| [为什么](docs/01-why.md) | 问题、真实代价、现有工具为何不合身 |
| [是什么](docs/02-design.md) | API、数据结构、进程模型、策略 |

## 安装

```bash
pip install tinyray                    # wheel 里带着注册中心
tinyray --listen 127.0.0.1:8760
```

## 开发

```bash
cargo build --release          # 需要 rustup 的 rustc，系统自带的太旧
maturin develop --release
pytest tests/ -q               # 默认集
pytest tests/ -q -m examples   # 示例，几分钟
cargo test --workspace         # registry 与线上消息类型
python bench.py                # 基准套件
python bench.py --check        # 与记录下来的基线比对
python mutation_check.py       # 把每个 bug 放回去，证明有测试会变红
mkdocs serve                   # 文档站，需要 pip install mkdocs-material
```

Python 测试按子系统组织：

| 目录 | 覆盖范围 |
|---|---|
| `tests/membership/` | 加入、就绪、座位、身份、清理和 fork 所有权 |
| `tests/discovery/` | 缓存查询、筛选、订阅和等待 |
| `tests/collectives/` | Epoch 和名单指纹 |
| `tests/registry/` | 协议、准入、租约、发布顺序和网络恢复 |
| `tests/rpc/` | 调用、校验、HTTP、载荷、并发和调用统计 |
| `tests/examples/` | 示例程序及其领域逻辑 |
| `tests/project/` | 公开 API、文档和 CI 契约 |

共享 fixture 在 `tests/conftest.py`，注册中心进程和网络代理在 `tests/support/`。
回归用例放在对应功能旁边，不再按里程碑或审查批次分文件。例如
`pytest tests/rpc/ -q` 只跑 RPC 子系统。Rust 测试仍放在各自的 crate 中。

## 它是怎么搭的

约 2,900 行：Rust 写的注册中心和客户端（`crates/`），外面是 Python API
（`python/tinyray/`），用 pyo3 和 maturin 缝在一起。

这里每一条行为都是先量后改的，而且每一条都在 `mutation_check.py` 里有条目 ——
把 bug 放回去，某条指名的测试就会变红。**一条不可能失败的测试不是测试。**
