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

```bash
pip install tinyray
tinyray --listen 127.0.0.1:8760
```

[快速上手](getting-started.md){ .md-button .md-button--primary }
[API 参考](api.md){ .md-button }

---

## 它替你解决什么

- **谁在** —— 报到、租约、本地缓存的名册。查询不走网络，所以查多少次都不加压。
- **还活着吗** —— 任期号与围栏。座位换了人，拿着旧地址的调用会被明确拒绝，
  而不是打到错的进程上。
- **我该找谁** —— 按名字、座位或 state 过滤找人，然后直接调它的方法。
- **一起点名** —— 冻住一轮名单，让每个 rank 拿到的必然相同。

底下就是普通 HTTP，`curl` 排障的本事一点没丢。

## 它刻意不做

任务队列、调度器、结果存储、数据面。**按任务登记会把它淹掉**，而 tensor 和权重
本来就不该经过控制面。这两条在[「是什么」](02-design.md)里有实测支撑。

需要带租约的工作队列，就在 tinyray 之上写一个库：它给你成员、任期围栏和变化
通知，队列自己拥有 job 身份、payload、结果和重试策略 —— 因为只有应用知道一个
job 是什么、结果能有多大、能不能重跑。

## 状态

**0.13.3 已发布**（[PyPI](https://pypi.org/project/tinyray/)），约 2,900 行。
提供 py3.10–3.13 的 Linux x86_64 / aarch64 与 macOS universal2 wheel。

多机与规模均已实测：三个容器跨网络命名空间互相发现并调用，指纹一致；10 万成员
跑通且订阅方看到全部，version 恰好 100,000（空转心跳一次都不算变更）；峰值
216,335 次/秒零错误（容器 + 网桥）。

## 文档

| 文档 | 内容 |
|---|---|
| [上手](getting-started.md) | 十分钟，从装上到两个进程互相调用 |
| [API 参考](api.md) | 完整接口，对着实现写的 |
| [基准](bench.md) | 它花多少钱，量出来的，以及量它时的坑 |
| [为什么](01-why.md) | 问题、真实代价、现有工具为何不合身、事故与原则 |
| [是什么](02-design.md) | pool 策略、边界与调用、scale 与保证 |

英文版覆盖面向使用者的四篇（[English](en/index.md)）；「为什么」和「是什么」
是设计笔记，只有中文。

## 写文档的规矩

- **每个数字必须标来源**：**实测**（benchmark 跑出来的）/ **推导**（算式可复算）/
  **待测**（冻结设计前必须拿到）。**没标注的数字是伪装过的猜测。**
- **面向使用者的文档中英各一份**（上手、API、基准）；设计笔记只有中文。
  代码、标识符和代码注释一律英文 —— 一个仓库混两种语言更糟。
- 固定术语保持英文：Pool、Slot、Incarnation、Lease、Registry、fencing、
  readiness、membership。
- **能用大白话就别用术语。**「合唱团少一个人全体卡住」比
  「collective 语义下单成员故障导致组内阻塞」好懂。
