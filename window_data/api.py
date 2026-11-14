"""窗口查询入口：规范化 → 共享排序 → 帧求值 → 结果回到原行id顺序。

资源约定：

- 内存以行槽计量，各结构上限由 Budget 派生，实际峰值见 stats['memory']；
- 溢写段集中在本次执行独占的临时目录，QueryResult.close()（或 with
  结束、迭代自然耗尽）即清理；调用者提前终止同样清理本次临时资源；
- 读写量、归并趟数、句柄峰值等见 stats['spill'] / stats['sorter']。
"""
from functools import cmp_to_key
from .io import DataError
from .spill import Budget, RowMeter, SpillManager, SpillableBuffer
from .sorter import ExternalSorter
from .winspec import group_windows, normalize_query, record_comparator
from .executor import execute_group


class QueryResult:
    """流式查询结果；关闭时保证清理本次全部临时资源。"""

    def __init__(self, gen, spill, meter, stats):
        self._gen = gen
        self._spill = spill
        self._meter = meter
        self._stats = stats
        self._closed = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration
        try:
            return next(self._gen)
        except StopIteration:
            self.close()
            raise

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._gen.close()
        finally:
            self._spill.close()
            self._finalize()

    def _finalize(self):
        stats = self._stats
        stats['memory'] = {
            'peak_rows': self._meter.peak,
            'by_tag': dict(sorted(self._meter.by_tag.items())),
        }
        stats['spill'] = self._spill.stats()
        stats['eval'] = {k: sorted(v) for k, v in stats['eval'].items()}
        stats['temp']['cleaned'] = self._spill.cleaned

    @property
    def stats(self):
        return self._stats

    @property
    def spill(self):
        """本次执行的溢写管理器（诊断与测试用）。"""
        return self._spill

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def run_window_query(rows, windows, functions, *, budget_rows=1024, spill_dir=None):
    """执行窗口查询，返回 QueryResult（输出按行id字典序）。

    rows：行迭代器（dict，含 id 及窗口引用列）；windows/functions 为
    未规范化的定义（见 winspec.normalize_query）。
    """
    specs, fns = normalize_query(windows, functions)
    budget = Budget(budget_rows)
    spill = SpillManager(spill_dir)
    meter = RowMeter()
    groups = group_windows(specs)
    stats = {
        'sort_groups': len(groups),
        'rows': 0,
        'rows_processed': 0,
        'partitions': 0,
        'records': 0,
        'sorter': {'runs': 0, 'merge_passes': 0, 'rows_sorted': 0},
        'eval': {'sliding': set(), 'recompute': set(), 'fallbacks': set()},
        'temp': {'dir': spill.dir, 'cleaned': False},
    }
    gen = _pipeline(rows, groups, fns, budget, spill, meter, stats)
    return QueryResult(gen, spill, meter, stats)


def _count_input(rows, stats):
    for row in rows:
        stats['rows'] += 1
        yield row


def _pipeline(rows, groups, fns, budget, spill, meter, stats):
    fns_by_window = {}
    for fn in fns:
        fns_by_window.setdefault(fn.window, []).append(fn)
    record_sorter = ExternalSorter(spill, meter, budget, stats['sorter'], tag='records')

    def records():
        if len(groups) <= 1:
            for signature, wins in groups:
                yield from execute_group(
                    _count_input(rows, stats), signature, wins,
                    fns_by_window, budget, spill, meter, stats)
            return
        # 多个排序组：输入落盘一次，逐组重读，不为每组驻留全量输入。
        source = SpillableBuffer(spill, meter, 'input', budget.segment_rows, budget.cache_segments)
        try:
            for row in _count_input(rows, stats):
                source.append(row)
            for signature, wins in groups:
                yield from execute_group(
                    source.iter_items(), signature, wins,
                    fns_by_window, budget, spill, meter, stats)
        finally:
            source.close()

    sorted_records = record_sorter.sort(records(), cmp_to_key(record_comparator))
    current_id = None
    current = None
    last_record_key = None
    for rec in sorted_records:
        stats['records'] += 1
        rid = rec['i']
        record_key = (rid, rec['k'])
        if record_key == last_record_key:
            raise DataError(f'行id重复或结果名冲突: {rid}')
        last_record_key = record_key
        if rid != current_id:
            if current is not None:
                yield current
            current_id = rid
            current = {'id': rid, 'results': {}}
        current['results'][rec['k']] = rec['v']
    if current is not None:
        yield current


def collect(result):
    """把 QueryResult 收拢为 (输出行列表, 统计)；保证关闭与清理。"""
    with result:
        out = list(result)
    return out, result.stats
