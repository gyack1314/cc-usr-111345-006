"""窗口与函数定义的规范化、排序键比较及可共享排序签名。

语义约定（与 docs/data-contract.md 一致）：

- partition 列值完全相等（null 与 null 视为相等）的行划入同一分区；
- order 列按各自的 direction/nulls 独立比较，业务值相同（null 视同相等）
  的行互为 peer；物理行顺序只以行 id 字典序打破并列，不拆散 peer 关系；
- start/end 为相对当前行或 peer 组的偏移：负值=PRECEDING，0=CURRENT，
  正值=FOLLOWING；RANGE 偏移按排序方向解释，null 排序键只与 null 成帧。
"""
from dataclasses import dataclass
from .io import DataError

UNITS = ('rows', 'groups', 'range')
EXCLUDES = ('none', 'current', 'group', 'ties')
AGG_KINDS = ('count', 'sum', 'avg', 'min', 'max')
RANK_KINDS = ('row_number', 'rank', 'dense_rank')
NAV_KINDS = ('lead', 'lag')
_DIRECTIONS = ('asc', 'desc')
_NULLS = ('first', 'last')


@dataclass(frozen=True)
class OrderKey:
    field: str
    direction: str
    nulls: str


@dataclass(frozen=True)
class WindowSpec:
    """规范化后的窗口定义；signature 相同的窗口共享一次排序。"""
    wid: str
    partition: tuple
    order: tuple
    unit: str
    start: int
    end: int
    exclude: str

    @property
    def signature(self):
        return (self.partition, self.order)


@dataclass(frozen=True)
class FunctionSpec:
    """规范化后的窗口函数调用。"""
    window: str
    kind: str
    field: object
    offset: object
    default: object
    name: str


def _require_int(value, what):
    if isinstance(value, bool) or not isinstance(value, int):
        raise DataError(f'{what}必须为整数')
    return value


def _require_str(value, what):
    if not isinstance(value, str) or not value:
        raise DataError(f'{what}必须为非空字符串')
    return value


def normalize_window(raw):
    if not isinstance(raw, dict):
        raise DataError('窗口定义必须是对象')
    wid = _require_str(raw.get('id'), '窗口id')
    part = raw.get('partition')
    if not isinstance(part, (list, tuple)):
        raise DataError(f'窗口{wid}的partition必须是数组')
    partition = tuple(_require_str(f, f'窗口{wid}的partition列') for f in part)
    order_raw = raw.get('order')
    if not isinstance(order_raw, (list, tuple)):
        raise DataError(f'窗口{wid}的order必须是数组')
    order = []
    for item in order_raw:
        if not isinstance(item, dict):
            raise DataError(f'窗口{wid}的order项必须是对象')
        field = _require_str(item.get('field'), f'窗口{wid}的排序列')
        direction = item.get('direction', 'asc')
        if direction not in _DIRECTIONS:
            raise DataError(f'窗口{wid}的排序方向无效: {direction}')
        nulls = item.get('nulls')
        if nulls is None:
            nulls = 'last' if direction == 'asc' else 'first'
        if nulls not in _NULLS:
            raise DataError(f'窗口{wid}的null位置无效: {nulls}')
        order.append(OrderKey(field, direction, nulls))
    order = tuple(order)
    unit = raw.get('unit')
    if unit not in UNITS:
        raise DataError(f'窗口{wid}的帧单位无效: {unit}')
    start = _require_int(raw.get('start'), f'窗口{wid}的帧起点')
    end = _require_int(raw.get('end'), f'窗口{wid}的帧终点')
    if start > end:
        raise DataError(f'窗口{wid}的帧起点晚于终点')
    exclude = raw.get('exclude', 'none')
    if exclude not in EXCLUDES:
        raise DataError(f'窗口{wid}的排除方式无效: {exclude}')
    if unit == 'range' and len(order) != 1:
        raise DataError(f'窗口{wid}的RANGE帧要求单个整数排序键')
    return WindowSpec(wid, partition, order, unit, start, end, exclude)


def normalize_function(raw, window_ids):
    if not isinstance(raw, dict):
        raise DataError('函数定义必须是对象')
    window = _require_str(raw.get('window'), '函数引用的窗口')
    if window not in window_ids:
        raise DataError(f'函数引用了未登记的窗口: {window}')
    kind = raw.get('kind')
    if kind not in AGG_KINDS + RANK_KINDS + NAV_KINDS:
        raise DataError(f'未知的窗口函数: {kind}')
    field = raw.get('field')
    if kind in AGG_KINDS + NAV_KINDS:
        field = _require_str(field, f'函数{kind}的取值列')
    elif field is not None:
        raise DataError(f'函数{kind}不接受取值列')
    offset = raw.get('offset')
    default = None
    if kind in NAV_KINDS:
        offset = _require_int(offset, f'函数{kind}的偏移')
        if offset < 0:
            raise DataError(f'函数{kind}的偏移不能为负')
        if 'default' not in raw:
            raise DataError(f'函数{kind}需要明确的越界默认值')
        default = raw['default']
    name = raw.get('name')
    if name is None:
        if kind in NAV_KINDS:
            name = f'{window}:{kind}({field},{offset})'
        elif kind in AGG_KINDS:
            name = f'{window}:{kind}({field})'
        else:
            name = f'{window}:{kind}'
    _require_str(name, '函数结果名')
    return FunctionSpec(window, kind, field, offset, default, name)


def normalize_query(windows, functions):
    """校验整个查询：窗口id唯一、函数引用已登记窗口、结果名唯一。"""
    specs = [normalize_window(w) for w in windows]
    ids = set()
    for spec in specs:
        if spec.wid in ids:
            raise DataError(f'窗口id重复: {spec.wid}')
        ids.add(spec.wid)
    fns = [normalize_function(f, ids) for f in functions]
    names = set()
    for fn in fns:
        if fn.name in names:
            raise DataError(f'函数结果名重复: {fn.name}')
        names.add(fn.name)
    return specs, fns


def group_windows(specs):
    """按 (partition, order) 签名分组；同组共享排序，帧状态各自独立。"""
    groups = {}
    for spec in specs:
        groups.setdefault(spec.signature, []).append(spec)
    return list(groups.items())


def row_value(row, field):
    try:
        return row[field]
    except KeyError as exc:
        raise DataError(f'行缺少字段: {field}') from exc


def compare_scalar(a, b):
    """非 null 值比较；类型不一致或不可比较时抛 DataError。"""
    if isinstance(a, bool) or isinstance(b, bool):
        if type(a) is not type(b):
            raise DataError('排序键类型不一致')
        return (a > b) - (a < b)
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return (a > b) - (a < b)
    if type(a) is not type(b):
        raise DataError('排序键类型不一致')
    try:
        return (a > b) - (a < b)
    except TypeError as exc:
        raise DataError('排序键值不可比较') from exc


def row_comparator(partition, order):
    """分区列(asc, nulls first) + 业务排序键 + 行id字典序的稳定比较器。

    行 id 只用于打破物理行顺序，不参与 peer 判定。
    """
    keys = [(f, 'asc', 'first') for f in partition]
    keys += [(k.field, k.direction, k.nulls) for k in order]
    keys.append(('id', 'asc', 'first'))

    def cmp(a, b):
        for field, direction, nulls in keys:
            va = row_value(a, field)
            vb = row_value(b, field)
            if va is None or vb is None:
                if va is None and vb is None:
                    continue
                return -1 if (va is None) == (nulls == 'first') else 1
            c = compare_scalar(va, vb)
            if c:
                return -c if direction == 'desc' else c
        return 0

    return cmp


def peers(a, b, order):
    """peer 判定：全部排序键业务值相等（null 视同相等），与方向无关。"""
    for k in order:
        va = row_value(a, k.field)
        vb = row_value(b, k.field)
        if va is None or vb is None:
            if va is not vb:
                return False
        elif va != vb:
            return False
    return True


def record_comparator(a, b):
    """结果记录按 (行id, 结果名) 字典序，用于输出前回到原行id顺序。"""
    if a['i'] != b['i']:
        return -1 if a['i'] < b['i'] else 1
    if a['k'] != b['k']:
        return -1 if a['k'] < b['k'] else 1
    return 0
