"""有界内存的溢写段、分段缓冲、预算与行槽计量。

- SpillManager：一次执行独占的临时目录，段文件带 SHA256 摘要，
  损坏（内容被改、截断、行数不符）在读取时必须抛 DataError；
  close() 清理本次全部临时资源，提前终止同样生效。
- SpillableBuffer：定长分段 + 有界 LRU 缓存的行缓冲，单个 peer 组
  超过内存预算时整体落盘，按下标顺序访问。
- RowMeter：以"行槽"为单位计量各结构的实际驻留峰值。
"""
import hashlib
import json
import os
import shutil
import tempfile
from collections import OrderedDict
from .io import DataError


class Budget:
    """内存预算（单位：行槽）。各结构的上限都由它派生。"""

    def __init__(self, rows=1024):
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 4:
            raise DataError('内存预算必须是不小于4的整数')
        self.rows = rows
        self.run_rows = rows                 # 排序单次内存运行
        self.segment_rows = max(2, rows // 2)  # 缓冲分段长度
        self.cache_segments = 2              # 每个缓冲的驻留段数
        self.fan_in = max(2, min(8, rows))   # 归并扇入 = 打开句柄上限
        self.deque_cap = rows                # min/max 单调队列上限

    def bound(self, minmax_functions=0):
        """文档化的驻留行槽上界，供调用者与测试核对实际峰值。"""
        buffers = 3 * (self.cache_segments * self.segment_rows + self.segment_rows)
        return 2 * self.run_rows + self.fan_in + buffers + minmax_functions * self.deque_cap + 8


class RowMeter:
    """按标签统计驻留行槽，记录总峰值与各标签峰值。"""

    def __init__(self):
        self._cur = {}
        self.peak = 0
        self.by_tag = {}

    def hold(self, tag, n):
        if n <= 0:
            return
        cur = self._cur.get(tag, 0) + n
        self._cur[tag] = cur
        if cur > self.by_tag.get(tag, 0):
            self.by_tag[tag] = cur
        total = sum(self._cur.values())
        if total > self.peak:
            self.peak = total

    def release(self, tag, n):
        if n <= 0:
            return
        left = self._cur.get(tag, 0) - n
        if left > 0:
            self._cur[tag] = left
        else:
            self._cur.pop(tag, None)

    def current(self, tag):
        return self._cur.get(tag, 0)


class SegmentRef:
    __slots__ = ('path', 'rows', 'digest', 'nbytes')

    def __init__(self, path, rows, digest, nbytes):
        self.path = path
        self.rows = rows
        self.digest = digest
        self.nbytes = nbytes


class SegmentReader:
    """流式读取溢写段，读尽时校验行数与摘要；损坏即 DataError。"""

    def __init__(self, mgr, ref):
        self._mgr = mgr
        self._ref = ref
        self._hash = hashlib.sha256()
        self._count = 0
        self._done = False
        self._counted_io = False
        self._fh = open(ref.path, 'rb')
        mgr._track_open()
        self._pending = self._fh.readline()

    def __iter__(self):
        return self

    def __next__(self):
        if self._done:
            raise StopIteration
        line = self._pending
        if line == b'':
            self.close()
            raise DataError('溢写段损坏：缺少摘要行')
        nxt = self._fh.readline()
        if nxt == b'':
            self._verify(line)
            self._done = True
            self.close()
            raise StopIteration
        self._hash.update(line)
        self._count += 1
        self._pending = nxt
        try:
            return json.loads(line)
        except ValueError as exc:
            self.close()
            raise DataError('溢写段损坏：行不是有效JSON') from exc

    def _verify(self, trailer_line):
        try:
            trailer = json.loads(trailer_line)
        except ValueError as exc:
            raise DataError('溢写段损坏：摘要行无效') from exc
        if not isinstance(trailer, dict) or trailer.get('rows') != self._count:
            raise DataError('溢写段摘要不一致：行数不符')
        if trailer.get('sha256') != self._hash.hexdigest() or trailer.get('sha256') != self._ref.digest:
            raise DataError('溢写段摘要不一致')

    def close(self):
        fh = self._fh
        if fh is not None:
            self._fh = None
            fh.close()
            self._mgr._track_close()
        if not self._counted_io:
            self._counted_io = True
            self._mgr.bytes_read += self._ref.nbytes

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class SpillManager:
    """一次执行独占的溢写目录；负责段写入、读取、释放与整体清理。"""

    def __init__(self, base_dir=None):
        try:
            self._dir = tempfile.mkdtemp(prefix='window-spill-', dir=base_dir)
        except OSError as exc:
            raise DataError('溢写临时目录不可用') from exc
        self._files = set()
        self._seq = 0
        self._open = 0
        self.peak_open = 0
        self.segments_written = 0
        self.bytes_written = 0
        self.bytes_read = 0
        self._closed = False

    @property
    def dir(self):
        return self._dir

    @property
    def cleaned(self):
        return self._closed and not os.path.exists(self._dir)

    def _track_open(self):
        self._open += 1
        if self._open > self.peak_open:
            self.peak_open = self._open

    def _track_close(self):
        self._open -= 1

    def write_segment(self, items, tag='seg'):
        self._seq += 1
        path = os.path.join(self._dir, f'{self._seq:06d}-{tag}.seg')
        body = bytearray()
        count = 0
        for item in items:
            body += json.dumps(item, ensure_ascii=False).encode('utf-8') + b'\n'
            count += 1
        digest = hashlib.sha256(bytes(body)).hexdigest()
        trailer = json.dumps({'rows': count, 'sha256': digest}).encode('utf-8')
        tmp = path + '.tmp'
        with open(tmp, 'wb') as fh:
            fh.write(bytes(body))
            fh.write(trailer + b'\n')
        os.replace(tmp, path)
        self._files.add(path)
        nbytes = len(body) + len(trailer) + 1
        self.bytes_written += nbytes
        self.segments_written += 1
        return SegmentRef(path, count, digest, nbytes)

    def open_segment(self, ref):
        return SegmentReader(self, ref)

    def read_segment(self, ref):
        with self.open_segment(ref) as reader:
            return list(reader)

    def release(self, ref):
        try:
            os.remove(ref.path)
        except OSError:
            pass
        self._files.discard(ref.path)

    def close(self):
        if self._closed:
            return
        self._closed = True
        for path in list(self._files):
            try:
                os.remove(path)
            except OSError:
                pass
        self._files.clear()
        shutil.rmtree(self._dir, ignore_errors=True)

    def stats(self):
        return {
            'segments': self.segments_written,
            'bytes_written': self.bytes_written,
            'bytes_read': self.bytes_read,
            'peak_open_files': self.peak_open,
        }

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class SpillableBuffer:
    """定长分段行缓冲：超出驻留段数的分段落盘，按下标访问时换入。"""

    def __init__(self, spill, meter, tag, segment_rows, cache_segments=2):
        self._spill = spill
        self._meter = meter
        self._tag = tag
        self._seg = segment_rows
        self._cache_cap = cache_segments
        self._cur = []
        self._segments = []
        self._cache = OrderedDict()
        self._total = 0
        self._closed = False

    def append(self, item):
        self._cur.append(item)
        self._total += 1
        self._meter.hold(self._tag, 1)
        if len(self._cur) >= self._seg:
            self._flush()

    def _flush(self):
        if not self._cur:
            return
        ref = self._spill.write_segment(self._cur, self._tag)
        self._meter.release(self._tag, len(self._cur))
        self._segments.append(ref)
        self._cur = []

    def __len__(self):
        return self._total

    def __getitem__(self, index):
        if index < 0 or index >= self._total:
            raise IndexError(index)
        seg_idx, off = divmod(index, self._seg)
        if seg_idx == len(self._segments):
            return self._cur[off]
        return self._load(seg_idx)[off]

    def _load(self, seg_idx):
        cached = self._cache.get(seg_idx)
        if cached is not None:
            self._cache.move_to_end(seg_idx)
            return cached
        data = self._spill.read_segment(self._segments[seg_idx])
        self._cache[seg_idx] = data
        self._meter.hold(self._tag, len(data))
        while len(self._cache) > self._cache_cap:
            _, evicted = self._cache.popitem(last=False)
            self._meter.release(self._tag, len(evicted))
        return data

    def iter_items(self):
        for i in range(self._total):
            yield self[i]

    def close(self):
        if self._closed:
            return
        self._closed = True
        for ref in self._segments:
            self._spill.release(ref)
        self._segments = []
        for data in self._cache.values():
            self._meter.release(self._tag, len(data))
        self._cache.clear()
        self._meter.release(self._tag, len(self._cur))
        self._cur = []
