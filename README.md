# tinyray

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

## 现状

**M1 + M2 已实现。** 报到、租约、本地缓存、找人，以及调用层：

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

**M3（编组与 `epoch`）还没写。**

## 文档

| 文档 | 内容 |
|---|---|
| [docs/01-why.md](docs/01-why.md) | 为什么 —— 问题、真实代价、现有工具为何不合身 |
| [docs/02-design.md](docs/02-design.md) | 是什么 —— API、数据结构、进程模型、策略 |
| [docs/03-plan.md](docs/03-plan.md) | 怎么做 —— 计划、代码预算、未测项 |

## 开发

```bash
cargo build --release          # 需要 rustup 的 rustc，系统自带的太旧
maturin develop --release
pytest tests/ -q
```
