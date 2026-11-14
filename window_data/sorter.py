"""有界内存与有界文件句柄的外排归并排序。

- 运行段长度不超过 budget.run_rows，装得下全部输入时不落盘；
- 归并扇入不超过 budget.fan_in（即同时打开的段句柄上限），
  运行段过多时分多趟归并，中间段读尽即删；
- 段读取走 SpillManager 的摘要校验，损坏的溢写段在此必然失败。
"""
from heapq import heappush, heappop
from itertools import islice


class ExternalSorter:
    def __init__(self, spill, meter, budget, counters, tag='rows'):
        self._spill = spill
        self._meter = meter
        self._run_rows = budget.run_rows
        self._fan_in = budget.fan_in
        self._counters = counters
        self._tag = tag
        self._run_tag = f'{tag}.run'
        self._merge_tag = f'{tag}.merge'

    def sort(self, iterable, key):
        """返回按 key 有序的输出迭代器；key 为 cmp_to_key 风格键包装。"""
        return self._run(iterable, key)

    def _run(self, iterable, key):
        it = iter(iterable)
        runs = []
        try:
            first = True
            while True:
                chunk = []
                for item in islice(it, self._run_rows):
                    chunk.append(item)
                    self._meter.hold(self._run_tag, 1)
                if not chunk:
                    break
                chunk.sort(key=key)
                self._counters['rows_sorted'] += len(chunk)
                if first and len(chunk) < self._run_rows:
                    for item in chunk:
                        yield item
                    self._meter.release(self._run_tag, len(chunk))
                    return
                first = False
                ref = self._spill.write_segment(chunk, f'{self._tag}.run')
                self._counters['runs'] += 1
                self._meter.release(self._run_tag, len(chunk))
                chunk = []
                runs.append(ref)
            while len(runs) > self._fan_in:
                merged = []
                for i in range(0, len(runs), self._fan_in):
                    group = runs[i:i + self._fan_in]
                    if len(group) == 1:
                        merged.append(group[0])
                        continue
                    ref = self._spill.write_segment(
                        self._merge_stream(group, key), f'{self._tag}.merge')
                    self._counters['runs'] += 1
                    self._counters['merge_passes'] += 1
                    for old in group:
                        self._spill.release(old)
                    merged.append(ref)
                runs = merged
            for item in self._merge_stream(runs, key):
                yield item
        finally:
            held = self._meter.current(self._run_tag)
            if held:
                self._meter.release(self._run_tag, held)
            for ref in runs:
                self._spill.release(ref)

    def _merge_stream(self, refs, key):
        readers = []
        heap = []
        self._meter.hold(self._merge_tag, len(refs))
        try:
            for idx, ref in enumerate(refs):
                reader = self._spill.open_segment(ref)
                readers.append(reader)
                try:
                    value = next(reader)
                except StopIteration:
                    continue
                heappush(heap, (key(value), idx, value))
            while heap:
                _, idx, value = heappop(heap)
                yield value
                try:
                    nxt = next(readers[idx])
                except StopIteration:
                    continue
                heappush(heap, (key(nxt), idx, nxt))
        finally:
            for reader in readers:
                reader.close()
            self._meter.release(self._merge_tag, len(refs))
