"""独立慢解释器：按定义逐行、逐帧直接求值，用于核对执行算子。

刻意不使用执行算子的游标、滑动状态与溢写结构：每行独立切片、
独立比较、独立聚合，宁可慢也要与 executor.py 的实现路径无关。
只要求小资料上逐行结果一致。
"""
from functools import cmp_to_key
from .io import DataError
from .winspec import normalize_query


def _cmp_keys(spec):
    keys = [(k.field, k.direction, k.nulls) for k in spec.order]

    def cmp(a, b):
        for field, direction, nulls in keys:
            va = a[field]
            vb = b[field]
            if va is None or vb is None:
                if va is None and vb is None:
                    continue
                return -1 if (va is None) == (nulls == 'first') else 1
            if type(va) is not type(vb) and not (
                    isinstance(va, (int, float)) and isinstance(vb, (int, float))
                    and not isinstance(va, bool) and not isinstance(vb, bool)):
                raise DataError('排序键类型不一致')
            c = (va > vb) - (va < vb)
            if c:
                return -c if direction == 'desc' else c
        ka = a['id']
        kb = b['id']
        return (ka > kb) - (ka < kb)

    return cmp


def _same_peer(a, b, spec):
    for k in spec.order:
        va = a[k.field]
        vb = b[k.field]
        if va is None or vb is None:
            if va is not vb:
                return False
        elif va != vb:
            return False
    return True


def _peer_groups(ordered, spec):
    groups = []
    start = 0
    for i in range(1, len(ordered)):
        if not _same_peer(ordered[i - 1], ordered[i], spec):
            groups.append((start, i))
            start = i
    groups.append((start, len(ordered)))
    return groups


def _frame_indices(spec, ordered, pos, groups):
    """按定义直接计算帧内行下标 [fs, fe)。"""
    n = len(ordered)
    if spec.unit == 'rows':
        fs = max(0, pos + spec.start)
        fe = min(n, pos + spec.end + 1)
        return (fs, max(fs, fe))
    if spec.unit == 'groups':
        g = next(i for i, (s, e) in enumerate(groups) if s <= pos < e)
        lo = g + spec.start
        hi = g + spec.end
        if lo > len(groups) - 1 or hi < 0:
            return (pos, pos)
        lo = max(0, lo)
        hi = min(len(groups) - 1, hi)
        return (groups[lo][0], groups[hi][1])
    # range：单个整数排序键，按方向解释偏移；null 只与 null 成帧
    key = spec.order[0]
    cur = ordered[pos][key.field]
    if cur is None:
        idx = [i for i, r in enumerate(ordered) if r[key.field] is None]
        return (idx[0], idx[-1] + 1) if idx else (pos, pos)
    if isinstance(cur, bool) or not isinstance(cur, int):
        raise DataError('RANGE排序键必须为整数或null')
    if key.direction == 'desc':
        lo_v, hi_v = cur - spec.end, cur - spec.start
    else:
        lo_v, hi_v = cur + spec.start, cur + spec.end
    idx = [i for i, r in enumerate(ordered)
           if r[key.field] is not None and lo_v <= r[key.field] <= hi_v]
    return (idx[0], idx[-1] + 1) if idx else (pos, pos)


def _aggregate(kind, field, values):
    nums = []
    for v in values:
        if v is None:
            continue
        if kind in ('sum', 'avg'):
            if isinstance(v, bool) or not isinstance(v, int):
                raise DataError('求和/均值字段必须为整数')
        nums.append(v)
    if kind == 'count':
        return len(nums)
    if not nums:
        return None
    if kind == 'sum':
        return sum(nums)
    if kind == 'avg':
        return sum(nums) / len(nums)
    if kind == 'min':
        best = nums[0]
        for v in nums[1:]:
            try:
                if v < best:
                    best = v
            except TypeError as exc:
                raise DataError('排序键值不可比较') from exc
        return best
    best = nums[0]
    for v in nums[1:]:
        try:
            if v > best:
                best = v
        except TypeError as exc:
            raise DataError('排序键值不可比较') from exc
    return best


def _eval_function(ordered, spec, fn, out):
    n = len(ordered)
    groups = _peer_groups(ordered, spec)
    for pos, row in enumerate(ordered):
        kind = fn.kind
        if kind == 'row_number':
            value = pos + 1
        elif kind in ('rank', 'dense_rank'):
            g = next(i for i, (s, e) in enumerate(groups) if s <= pos < e)
            value = groups[g][0] + 1 if kind == 'rank' else g + 1
        elif kind in ('lead', 'lag'):
            idx = pos + fn.offset if kind == 'lead' else pos - fn.offset
            value = ordered[idx][fn.field] if 0 <= idx < n else fn.default
        else:
            fs, fe = _frame_indices(spec, ordered, pos, groups)
            cur = ordered[pos]
            picked = []
            for i in range(fs, fe):
                other = ordered[i]
                if spec.exclude == 'current' and i == pos:
                    continue
                if spec.exclude == 'group' and _same_peer(other, cur, spec):
                    continue
                if spec.exclude == 'ties' and i != pos and _same_peer(other, cur, spec):
                    continue
                picked.append(other[fn.field])
            value = _aggregate(kind, fn.field, picked)
        out[row['id']][fn.name] = value


def reference_run(rows, windows, functions):
    """慢速但直接的参考结果：按行id字典序返回 [{'id', 'results'}]。"""
    specs, fns = normalize_query(windows, functions)
    by_id = {spec.wid: spec for spec in specs}
    rows = list(rows)
    out = {}
    for row in rows:
        out.setdefault(row['id'], {})
    for fn in fns:
        spec = by_id[fn.window]
        parts = {}
        for row in rows:
            key = tuple(row[f] for f in spec.partition)
            parts.setdefault(key, []).append(row)
        for part_rows in parts.values():
            ordered = sorted(part_rows, key=cmp_to_key(_cmp_keys(spec)))
            _eval_function(ordered, spec, fn, out)
    return [{'id': rid, 'results': out[rid]} for rid in sorted(out)]
