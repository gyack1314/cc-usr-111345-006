"""窗口帧推进与函数求值。

同序行（peer）的成帧与共享计算规则：

- 每个排序组先对分区做一次共享分析：peer 组边界、RANGE 键的 null 区间，
  供组内所有窗口使用；帧游标与聚合状态按窗口各自持有，互不串用；
- ROWS/GROUPS/RANGE 帧界随行位置单调前进，无排除项时用滑动聚合
  （sum/count/avg 为 O(1) 状态，min/max 用有界单调队列）；
- 排除项或检测到的非单调帧改用"按帧重算"策略：直接扫描帧内行，
  不套用只适用于累计帧的增量公式；min/max 队列超出预算同样回退重算；
- 排名函数（row_number/rank/dense_rank）与 lead/lag 不依赖聚合帧，
  lead/lag 保留 null、不跳过缺测行，越界返回调用方给定的明确默认值。
"""
from collections import deque
from functools import cmp_to_key
from .io import DataError
from .spill import SpillableBuffer
from .sorter import ExternalSorter
from .winspec import (
    NAV_KINDS, RANK_KINDS,
    compare_scalar, peers, row_comparator, row_value,
)


class _DequeOverflow(Exception):
    """min/max 单调队列超出预算，需回退重算。"""


class _NonMonotonicFrame(Exception):
    """帧界未按约定单调前进，需回退重算。"""


class _SharedPartition:
    """同一排序组内共享的分区级索引：peer 边界与 RANGE 键 null 区间。"""

    def __init__(self, n, boundaries, null_region, nn_region):
        self.n = n
        self.boundaries = boundaries
        self.null_region = null_region
        self.nn_region = nn_region

    def close(self):
        self.boundaries.close()


def _analyze(buffer, order, need_range_regions, spill, meter, budget):
    """一次顺序扫描建立 peer 组边界；RANGE 窗口存在时校验整数键并定位 null 区间。"""
    n = len(buffer)
    boundaries = SpillableBuffer(spill, meter, 'peer', budget.segment_rows, budget.cache_segments)
    start = 0
    prev = buffer[0]
    for i in range(1, n):
        row = buffer[i]
        if not peers(prev, row, order):
            boundaries.append([start, i])
            start = i
        prev = row
    boundaries.append([start, n])
    null_region = None
    nn_region = None
    if need_range_regions:
        field = order[0].field
        first_null = None
        last_null = None
        for i in range(n):
            value = row_value(buffer[i], field)
            if value is None:
                if first_null is None:
                    first_null = i
                last_null = i
            elif isinstance(value, bool) or not isinstance(value, int):
                raise DataError('RANGE排序键必须为整数或null')
        if first_null is None:
            null_region = (0, 0)
            nn_region = (0, n)
        elif first_null == 0:
            null_region = (0, last_null + 1)
            nn_region = (last_null + 1, n)
        else:
            null_region = (first_null, n)
            nn_region = (0, first_null)
    return _SharedPartition(n, boundaries, null_region, nn_region)


class _PeerLocator:
    """随行位置单调前进的定位器：行号 → peer 组下标。"""

    def __init__(self, shared):
        self._bounds = shared.boundaries
        self._groups = len(shared.boundaries)
        self.g = 0

    def group_of(self, pos):
        bounds = self._bounds
        while self.g + 1 < self._groups and pos >= bounds[self.g][1]:
            self.g += 1
        return self.g

    def span(self, g):
        return self._bounds[g]


class _RowsCursor:
    """ROWS 帧：偏移只按物理行计，CURRENT ROW 即当前行本身。"""

    def __init__(self, n, start, end):
        self.n = n
        self.start = start
        self.end = end

    def frame(self, pos):
        fs = pos + self.start
        fe = pos + self.end + 1
        if fs < 0:
            fs = 0
        if fe > self.n:
            fe = self.n
        if fs > self.n:
            fs = self.n
        if fe < 0:
            fe = 0
        if fe < fs:
            fe = fs
        return fs, fe


class _GroupsCursor:
    """GROUPS 帧：偏移按完整 peer 组计，CURRENT ROW 指整个 peer 组。"""

    def __init__(self, shared, start, end):
        self.shared = shared
        self.start = start
        self.end = end
        self.g = 0
        self.groups = len(shared.boundaries)

    def frame(self, pos):
        bounds = self.shared.boundaries
        while self.g + 1 < self.groups and pos >= bounds[self.g][1]:
            self.g += 1
        lo = self.g + self.start
        hi = self.g + self.end
        if lo > self.groups - 1 or hi < 0:
            return pos, pos
        if lo < 0:
            lo = 0
        if hi > self.groups - 1:
            hi = self.groups - 1
        return bounds[lo][0], bounds[hi][1]


class _RangeCursor:
    """RANGE 帧：单个整数排序键上的值域偏移，按方向解释。

    升序：值域 [cur+start, cur+end]；降序：值域 [cur-end, cur-start]。
    null 排序键不做加减，只与 null 行成帧（整个 null peer 区间）。
    """

    def __init__(self, buffer, shared, key, start, end):
        self.buf = buffer
        self.shared = shared
        self.field = key.field
        self.desc = key.direction == 'desc'
        self.start = start
        self.end = end
        ns, ne = shared.nn_region
        self.nn_end = ne
        self.fs = ns
        self.fe = ns

    def _val(self, i):
        return row_value(self.buf[i], self.field)

    def frame(self, pos):
        cur = self._val(pos)
        if cur is None:
            return self.shared.null_region
        if self.desc:
            near = cur - self.start
            far = cur - self.end
        else:
            near = cur + self.start
            far = cur + self.end
        fs = self.fs
        while fs < self.nn_end:
            v = self._val(fs)
            if (v > near) if self.desc else (v < near):
                fs += 1
            else:
                break
        fe = self.fe if self.fe > fs else fs
        while fe < self.nn_end:
            v = self._val(fe)
            if (v >= far) if self.desc else (v <= far):
                fe += 1
            else:
                break
        self.fs = fs
        self.fe = fe
        return fs, fe


def _make_cursor(win, buffer, shared):
    if win.unit == 'rows':
        return _RowsCursor(shared.n, win.start, win.end)
    if win.unit == 'groups':
        return _GroupsCursor(shared, win.start, win.end)
    return _RangeCursor(buffer, shared, win.order[0], win.start, win.end)


def _check_int(value, what):
    if isinstance(value, bool) or not isinstance(value, int):
        raise DataError(f'{what}必须为整数')
    return value


class _SlidingAgg:
    """count/sum/avg 的 O(1) 滑动状态；帧界必须单调前进。"""

    needs_frame = True

    def __init__(self, name, kind, buffer, field):
        self.name = name
        self.kind = kind
        self.buf = buffer
        self.field = field
        self.total = 0
        self.cnt = 0
        self.pfs = 0
        self.pfe = 0
        self.fresh = True

    def _add(self, i):
        v = row_value(self.buf[i], self.field)
        if v is None:
            return
        if self.kind in ('sum', 'avg'):
            _check_int(v, '求和/均值字段')
            self.total += v
        self.cnt += 1

    def _remove(self, i):
        v = row_value(self.buf[i], self.field)
        if v is None:
            return
        if self.kind in ('sum', 'avg'):
            self.total -= v
        self.cnt -= 1

    def row(self, pos, fs, fe, locator):
        if self.fresh:
            self.pfs = fs
            self.pfe = fs
            self.fresh = False
        if fs < self.pfs or fe < self.pfe:
            raise _NonMonotonicFrame
        i = self.pfs
        stop = min(fs, self.pfe)
        while i < stop:
            self._remove(i)
            i += 1
        i = max(self.pfe, fs)
        while i < fe:
            self._add(i)
            i += 1
        self.pfs = fs
        self.pfe = fe
        if self.kind == 'count':
            return self.cnt
        if self.cnt == 0:
            return None
        if self.kind == 'sum':
            return self.total
        return self.total / self.cnt


class _SlidingMinMax:
    """min/max 的有界单调队列；队列超预算抛 _DequeOverflow 回退重算。"""

    needs_frame = True

    def __init__(self, name, kind, buffer, field, cap, meter):
        self.name = name
        self.kind = kind
        self.buf = buffer
        self.field = field
        self.cap = cap
        self.meter = meter
        self.deq = deque()
        self.pfs = 0
        self.pfe = 0
        self.fresh = True

    def _better(self, a, b):
        c = compare_scalar(a, b)
        return c < 0 if self.kind == 'min' else c > 0

    def _add(self, i):
        v = row_value(self.buf[i], self.field)
        if v is None:
            return
        deq = self.deq
        while deq and self._better(v, deq[-1][1]):
            deq.pop()
            self.meter.release('deque', 1)
        deq.append((i, v))
        self.meter.hold('deque', 1)
        if len(deq) > self.cap:
            raise _DequeOverflow

    def row(self, pos, fs, fe, locator):
        if self.fresh:
            self.pfs = fs
            self.pfe = fs
            self.fresh = False
        if fs < self.pfs or fe < self.pfe:
            raise _NonMonotonicFrame
        deq = self.deq
        i = self.pfs
        stop = min(fs, self.pfe)
        while i < stop:
            if deq and deq[0][0] == i:
                deq.popleft()
                self.meter.release('deque', 1)
            i += 1
        i = max(self.pfe, fs)
        while i < fe:
            self._add(i)
            i += 1
        self.pfs = fs
        self.pfe = fe
        return deq[0][1] if deq else None

    def release(self):
        """回退重算前释放队列占用的行槽计量。"""
        self.meter.release('deque', len(self.deq))
        self.deq = deque()


class _RecomputeAgg:
    """排除项/非单调帧下的可验证重算：直接扫描帧内行聚合。

    不套用累计帧的增量公式；空帧 count=0、sum/avg 为 null，
    非空全 null 帧同样保持 null 而不变成 0。
    """

    needs_frame = True

    def __init__(self, name, kind, buffer, field, exclude):
        self.name = name
        self.kind = kind
        self.buf = buffer
        self.field = field
        self.exclude = exclude

    def row(self, pos, fs, fe, locator):
        exclude = self.exclude
        gs = ge = -1
        if exclude in ('group', 'ties'):
            gs, ge = locator.span(locator.group_of(pos))
        total = 0
        cnt = 0
        best = None
        for i in range(fs, fe):
            if exclude == 'current' and i == pos:
                continue
            if exclude == 'group' and gs <= i < ge:
                continue
            if exclude == 'ties' and i != pos and gs <= i < ge:
                continue
            v = row_value(self.buf[i], self.field)
            if v is None:
                continue
            if self.kind in ('sum', 'avg'):
                _check_int(v, '求和/均值字段')
                total += v
            elif self.kind in ('min', 'max'):
                if best is None or self._better(v, best):
                    best = v
            cnt += 1
        if self.kind == 'count':
            return cnt
        if cnt == 0:
            return None
        if self.kind == 'sum':
            return total
        if self.kind == 'avg':
            return total / cnt
        return best

    def _better(self, a, b):
        c = compare_scalar(a, b)
        return c < 0 if self.kind == 'min' else c > 0


class _RankState:
    """排名函数：只依赖 peer 组边界，与聚合帧无关。"""

    needs_frame = False

    def __init__(self, name, kind):
        self.name = name
        self.kind = kind

    def row(self, pos, fs, fe, locator):
        if self.kind == 'row_number':
            return pos + 1
        g = locator.group_of(pos)
        if self.kind == 'rank':
            return locator.span(g)[0] + 1
        return g + 1


class _NavState:
    """lead/lag：按物理行偏移取值，保留 null，越界给明确默认值。"""

    needs_frame = False

    def __init__(self, name, kind, buffer, field, offset, default, n):
        self.name = name
        self.kind = kind
        self.buf = buffer
        self.field = field
        self.offset = offset
        self.default = default
        self.n = n

    def row(self, pos, fs, fe, locator):
        idx = pos + self.offset if self.kind == 'lead' else pos - self.offset
        if 0 <= idx < self.n:
            return row_value(self.buf[idx], self.field)
        return self.default


def _make_state(fn, win, buffer, budget, meter, eval_stats):
    kind = fn.kind
    if kind in RANK_KINDS:
        return _RankState(fn.name, kind)
    if kind in NAV_KINDS:
        return _NavState(fn.name, kind, buffer, fn.field, fn.offset, fn.default, len(buffer))
    if win.exclude != 'none':
        eval_stats['recompute'].add(fn.name)
        return _RecomputeAgg(fn.name, kind, buffer, fn.field, win.exclude)
    eval_stats['sliding'].add(fn.name)
    if kind in ('min', 'max'):
        return _SlidingMinMax(fn.name, kind, buffer, fn.field, budget.deque_cap, meter)
    return _SlidingAgg(fn.name, kind, buffer, fn.field)


def evaluate_window(win, fns, buffer, shared, budget, meter, eval_stats):
    """对单个窗口逐行求值，产出 (行id, 结果名, 值) 记录流。"""
    states = [_make_state(fn, win, buffer, budget, meter, eval_stats) for fn in fns]
    if not states:
        return
    locator = _PeerLocator(shared)
    cursor = _make_cursor(win, buffer, shared) if any(st.needs_frame for st in states) else None
    for pos in range(shared.n):
        fs, fe = cursor.frame(pos) if cursor is not None else (0, 0)
        rid = row_value(buffer[pos], 'id')
        for idx, st in enumerate(states):
            try:
                value = st.row(pos, fs, fe, locator)
            except (_DequeOverflow, _NonMonotonicFrame):
                # 回退到可验证的重算策略，当前行起生效；不回套增量公式。
                if isinstance(st, _SlidingMinMax):
                    st.release()
                st = _RecomputeAgg(st.name, st.kind, buffer, st.field, 'none')
                states[idx] = st
                eval_stats['fallbacks'].add(st.name)
                value = st.row(pos, fs, fe, locator)
            yield (rid, st.name, value)


def _partitions_of(rows_iter, partition, spill, meter, budget):
    """把有序行流切成分区，逐分区装入可溢写缓冲。"""
    buf = None
    key = None
    try:
        for row in rows_iter:
            row_key = tuple(row_value(row, f) for f in partition)
            if buf is not None and row_key != key:
                yield key, buf
                buf = None
            if buf is None:
                buf = SpillableBuffer(spill, meter, 'partition', budget.segment_rows, budget.cache_segments)
                key = row_key
            buf.append(row)
        if buf is not None:
            yield key, buf
            buf = None
    finally:
        if buf is not None:
            buf.close()


def execute_group(rows_iter, signature, wins, fns_by_window, budget, spill, meter, stats):
    """单个排序组：一次共享排序 → 逐分区共享分析 → 各窗口独立求值。"""
    partition, order = signature
    sorter = ExternalSorter(spill, meter, budget, stats['sorter'], tag='rows')
    sorted_rows = sorter.sort(rows_iter, cmp_to_key(row_comparator(partition, order)))
    eval_stats = stats['eval']
    for _key, buf in _partitions_of(sorted_rows, partition, spill, meter, budget):
        try:
            need_range = any(w.unit == 'range' for w in wins)
            shared = _analyze(buf, order, need_range, spill, meter, budget)
            try:
                for win in wins:
                    fns = fns_by_window.get(win.wid, ())
                    for rid, name, value in evaluate_window(
                            win, fns, buf, shared, budget, meter, eval_stats):
                        yield {'i': rid, 'k': name, 'v': value}
            finally:
                shared.close()
            stats['partitions'] += 1
            stats['rows_processed'] += len(buf)
        finally:
            buf.close()
