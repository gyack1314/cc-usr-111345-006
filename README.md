# 用量仓窗口函数的同序帧与外存执行

本项目保存仓库与湖表开发所需的本地资料，提供内容摘要、记录身份与登记关系检查，并在登记资料上提供窗口执行算子：累计费用、滑动均值与并列排名可在同一查询中求值，内存与文件句柄有界，允许外排。

资料表包含rows、windows。字段单位和关系见 docs/data-contract.md。data/normal.json登记常规资料，data/boundary.json登记特殊业务情形，data/broken-digest.json用于检查资料损坏的拒绝行为。JSONL文件按清单内SHA256和字节长度绑定。

运行环境为 Python 3.11及以上，当前仅使用标准库，无需额外服务。包内io.py负责资料读取，domain.py负责表集合与引用检查，cli.py提供本地检查入口；winspec.py规范化窗口与函数定义，sorter.py提供外排归并，executor.py执行帧推进与函数求值，spill.py管理溢写段与计量，api.py是查询入口，reference.py是用于核对的独立慢解释器。

```sh
python -m unittest discover -s tests -v
python -m compileall -q window_data tests
python -m window_data.cli data/normal.json
python -m window_data.cli data/boundary.json
```

在登记窗口上执行演示查询（聚合、排名、lead/lag），输出含结果与统计：

```sh
python -m window_data.cli --query --budget-rows 1024 data/normal.json
```

程序接口：

```python
from window_data.api import run_window_query, collect
result = run_window_query(rows, windows, functions, budget_rows=1024)
with result:
    for row in result:      # {'id': 行id, 'results': {结果名: 值}}，按行id字典序
        ...
stats = result.stats        # 关闭后可读：峰值、读写、归并、清理状态
# 或一次性收拢：rows, stats = collect(run_window_query(...))
```

## 执行语义要点

- 同一 (partition, order) 签名的窗口共享一次排序；帧游标与聚合状态按窗口各自持有，互不串用。
- 业务排序键相同（null 视同相等）的行互为 peer；行 id 只打破物理行顺序，不拆散 rank 与 RANGE 的 peer 关系，同 peer 的累计值一致。
- ROWS/GROUPS/RANGE 帧界随行位置单调前进：无排除项时用滑动聚合（sum/count/avg 为 O(1) 状态，min/max 用有界单调队列）；有排除项或检测到非单调帧时改用按帧重算，不套用累计帧的增量公式；min/max 队列超预算同样回退重算。
- 空帧 COUNT 为 0、SUM/AVG 为 null；非空全 null 帧同样保持 null。lead/lag 保留 null、不跳过缺测行，越界返回调用方给定的明确默认值。
- 内存以行槽计量（Budget），排序运行段、缓冲分段、归并扇入、单调队列上限都由预算派生；单个 peer 组超过预算时整体溢写，stats['memory'] 给出实际峰值，stats['spill'] 给出读写量与句柄峰值。
- 溢写段带 SHA256 摘要，损坏即失败；QueryResult.close()（或 with 结束、迭代耗尽）清理本次全部临时资源，调用者提前终止同样生效。

容器内使用同样的检查入口：

```sh
docker build -t window-data .
docker run --rm window-data
```
