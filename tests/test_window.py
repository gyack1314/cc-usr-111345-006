"""窗口执行算子：语义、资源边界与跨实现核对。"""
import io as text_io
import json
import os
import random
import tempfile
import unittest
from contextlib import redirect_stdout

from window_data import cli
from window_data.api import collect, run_window_query
from window_data.io import DataError, load
from window_data.reference import reference_run
from window_data.spill import Budget, SpillManager
from window_data.winspec import normalize_query, normalize_window


def run(rows, windows, functions, budget_rows=1024):
    out, stats = collect(run_window_query(rows, windows, functions, budget_rows=budget_rows))
    return {r['id']: r['results'] for r in out}, stats


def demo_fns(wid, field='cost'):
    fns = [{'window': wid, 'kind': k, 'field': field}
           for k in ('count', 'sum', 'avg', 'min', 'max')]
    fns += [{'window': wid, 'kind': k} for k in ('row_number', 'rank', 'dense_rank')]
    fns.append({'window': wid, 'kind': 'lead', 'field': field, 'offset': 1, 'default': None})
    fns.append({'window': wid, 'kind': 'lag', 'field': field, 'offset': 1, 'default': None})
    return fns


class NormalizeTests(unittest.TestCase):
    def test_bad_unit_rejected(self):
        with self.assertRaisesRegex(DataError, '帧单位'):
            normalize_window({'id': 'w', 'partition': [], 'order': [],
                              'unit': 'rows+', 'start': -1, 'end': 0, 'exclude': 'none'})

    def test_frame_start_after_end_rejected(self):
        with self.assertRaisesRegex(DataError, '起点晚于终点'):
            normalize_window({'id': 'w', 'partition': [], 'order': [],
                              'unit': 'rows', 'start': 1, 'end': -1, 'exclude': 'none'})

    def test_range_requires_single_key(self):
        order = [{'field': 'time', 'direction': 'asc', 'nulls': 'last'},
                 {'field': 'cost', 'direction': 'asc', 'nulls': 'last'}]
        with self.assertRaisesRegex(DataError, '单个整数排序键'):
            normalize_window({'id': 'w', 'partition': [], 'order': order,
                              'unit': 'range', 'start': -1, 'end': 0, 'exclude': 'none'})

    def test_non_integer_offset_rejected(self):
        with self.assertRaisesRegex(DataError, '整数'):
            normalize_window({'id': 'w', 'partition': [], 'order': [],
                              'unit': 'rows', 'start': -1.5, 'end': 0, 'exclude': 'none'})

    def test_bad_exclude_rejected(self):
        with self.assertRaisesRegex(DataError, '排除方式'):
            normalize_window({'id': 'w', 'partition': [], 'order': [],
                              'unit': 'rows', 'start': -1, 'end': 0, 'exclude': 'others'})

    def test_function_requires_registered_window(self):
        with self.assertRaisesRegex(DataError, '未登记的窗口'):
            normalize_query([], [{'window': 'ghost', 'kind': 'row_number'}])

    def test_nav_requires_explicit_default(self):
        win = {'id': 'w', 'partition': [], 'order': [], 'unit': 'rows',
               'start': -1, 'end': 0, 'exclude': 'none'}
        with self.assertRaisesRegex(DataError, '默认值'):
            normalize_query([win], [{'window': 'w', 'kind': 'lead',
                                     'field': 'cost', 'offset': 1}])

    def test_duplicate_result_name_rejected(self):
        win = {'id': 'w', 'partition': [], 'order': [], 'unit': 'rows',
               'start': -1, 'end': 0, 'exclude': 'none'}
        fns = [{'window': 'w', 'kind': 'row_number', 'name': 'x'},
               {'window': 'w', 'kind': 'rank', 'name': 'x'}]
        with self.assertRaisesRegex(DataError, '结果名重复'):
            normalize_query([win], fns)

    def test_shared_signature_grouping(self):
        base = {'partition': ['tenant'],
                'order': [{'field': 'time', 'direction': 'asc', 'nulls': 'last'}]}
        w1 = dict(base, id='w1', unit='rows', start=-1, end=0, exclude='none')
        w2 = dict(base, id='w2', unit='groups', start=-2, end=2, exclude='ties')
        other_order = dict(base, id='w3', unit='rows', start=-1, end=0, exclude='none')
        other_order['order'] = [{'field': 'time', 'direction': 'desc', 'nulls': 'last'}]
        specs, _ = normalize_query([w1, w2, other_order], [])
        from window_data.winspec import group_windows
        groups = group_windows(specs)
        self.assertEqual(len(groups), 2)
        sizes = sorted(len(wins) for _, wins in groups)
        self.assertEqual(sizes, [1, 2])


class SemanticsTests(unittest.TestCase):
    def test_normal_material_hand_computed(self):
        tables = load('data/normal.json')
        got, _ = run(tables['rows'], tables['windows'], demo_fns('w1') + demo_fns('w2'))
        # w1: groups -1..0 exclude ties，升序 time nulls last
        self.assertEqual(got['r1']['w1:sum(cost)'], 3)
        self.assertIsNone(got['r2']['w1:sum(cost)'])
        self.assertEqual(got['r2']['w1:count(cost)'], 0)
        self.assertEqual(got['r3']['w1:sum(cost)'], 10)
        self.assertEqual(got['r4']['w1:sum(cost)'], 9)
        self.assertEqual(got['r4']['w1:avg(cost)'], 4.5)
        self.assertEqual((got['r3']['w1:rank'], got['r3']['w1:dense_rank']), (3, 2))
        # lead/lag 保留 null：r3 的 lag 是 r2 的 null cost
        self.assertIsNone(got['r3']['w1:lag(cost,1)'])
        self.assertIsNone(got['r1']['w1:lead(cost,1)'])
        # w2: range 10 preceding..current，降序 time nulls last
        self.assertEqual(got['r3']['w2:sum(cost)'], 7)
        self.assertEqual(got['r1']['w2:sum(cost)'], 10)
        self.assertEqual(got['r2']['w2:sum(cost)'], 10)
        self.assertEqual(got['r4']['w2:sum(cost)'], 2)  # null 只与 null 成帧
        self.assertEqual((got['r1']['w2:rank'], got['r2']['w2:rank']), (2, 2))

    def test_peers_share_cumulative_value(self):
        # 同一时刻的并发请求累计值必须一致（同事逐行切片曾在此处不一致）
        tables = load('data/boundary.json')
        got, _ = run(tables['rows'], tables['windows'], demo_fns('w2'))
        self.assertEqual(got['r1']['w2:sum(cost)'], 10)
        self.assertEqual(got['r2']['w2:sum(cost)'], 10)
        self.assertEqual(got['r3']['w2:sum(cost)'], 10)
        self.assertEqual(got['r1']['w2:rank'], got['r3']['w2:rank'])

    def test_empty_frame_and_all_null_frame(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1, 'v': 7},
                {'id': 'b', 'g': 1, 'k': 2, 'v': None},
                {'id': 'c', 'g': 1, 'k': 3, 'v': 5}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': -1, 'end': -1, 'exclude': 'none'}
        got, _ = run(rows, [win], demo_fns('w', 'v'))
        # a 的帧为空：count=0，sum/avg 为 null
        self.assertEqual(got['a']['w:count(v)'], 0)
        self.assertIsNone(got['a']['w:sum(v)'])
        self.assertIsNone(got['a']['w:avg(v)'])
        # b 的帧为 {a}：正常取值
        self.assertEqual(got['b']['w:sum(v)'], 7)
        self.assertEqual(got['b']['w:count(v)'], 1)
        # c 的帧只含 b 的 null：非空全 null 帧不能变成 0
        self.assertEqual(got['c']['w:count(v)'], 0)
        self.assertIsNone(got['c']['w:sum(v)'])
        self.assertIsNone(got['c']['w:min(v)'])

    def test_exclude_group_removes_whole_peer_group(self):
        rows = [{'id': f'r{i}', 'g': 1, 'k': 1, 'v': i} for i in range(3)]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'groups', 'start': 0, 'end': 0, 'exclude': 'group'}
        got, _ = run(rows, [win], demo_fns('w', 'v'))
        for i in range(3):
            self.assertEqual(got[f'r{i}']['w:count(v)'], 0)
            self.assertIsNone(got[f'r{i}']['w:sum(v)'])

    def test_exclude_ties_keeps_current_row(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1, 'v': 4},
                {'id': 'b', 'g': 1, 'k': 1, 'v': 6}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'groups', 'start': 0, 'end': 0, 'exclude': 'ties'}
        got, _ = run(rows, [win], demo_fns('w', 'v'))
        self.assertEqual(got['a']['w:sum(v)'], 4)
        self.assertEqual(got['b']['w:sum(v)'], 6)

    def test_exclude_current_row_only(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1, 'v': 4},
                {'id': 'b', 'g': 1, 'k': 1, 'v': 6}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': -1, 'end': 1, 'exclude': 'current'}
        got, _ = run(rows, [win], demo_fns('w', 'v'))
        self.assertEqual(got['a']['w:sum(v)'], 6)
        self.assertEqual(got['b']['w:sum(v)'], 4)

    def test_descending_range_offsets(self):
        rows = [{'id': f'r{v}', 'g': 1, 'k': v, 'v': v} for v in (10, 20, 30, 40)]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'desc', 'nulls': 'last'}],
               'unit': 'range', 'start': -15, 'end': 0, 'exclude': 'none'}
        got, _ = run(rows, [win], demo_fns('w', 'v'))
        # 降序：PRECEDING 指向更大的值；k=20 的帧为 [20, 35] → {20, 30}
        self.assertEqual(got['r20']['w:sum(v)'], 50)
        self.assertEqual(got['r40']['w:sum(v)'], 40)
        self.assertEqual(got['r10']['w:sum(v)'], 30)  # [10, 25] → {10, 20}

    def test_null_keys_form_own_peer_group(self):
        rows = [{'id': 'n1', 'g': 1, 'k': None, 'v': 1},
                {'id': 'n2', 'g': 1, 'k': None, 'v': 2},
                {'id': 'x', 'g': 1, 'k': 5, 'v': 4}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'first'}],
               'unit': 'range', 'start': -100, 'end': 100, 'exclude': 'none'}
        got, _ = run(rows, [win], demo_fns('w', 'v'))
        # null 行：帧只含 null peer，不做 null 加减
        self.assertEqual(got['n1']['w:sum(v)'], 3)
        self.assertEqual(got['n2']['w:sum(v)'], 3)
        # 非 null 行：帧不含 null 行
        self.assertEqual(got['x']['w:sum(v)'], 4)
        self.assertEqual((got['n1']['w:rank'], got['n2']['w:rank']), (1, 1))
        self.assertEqual(got['x']['w:rank'], 3)

    def test_rank_ties_not_broken_by_row_id(self):
        rows = [{'id': 'b', 'g': 1, 'k': 7, 'v': 1},
                {'id': 'a', 'g': 1, 'k': 7, 'v': 2},
                {'id': 'c', 'g': 1, 'k': 9, 'v': 3}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': 0, 'end': 0, 'exclude': 'none'}
        got, _ = run(rows, [win], demo_fns('w', 'v'))
        # 物理顺序由行 id 打破（a 在 b 前），但 rank/dense_rank 不被拆散
        self.assertEqual(got['a']['w:row_number'], 1)
        self.assertEqual(got['b']['w:row_number'], 2)
        self.assertEqual((got['a']['w:rank'], got['b']['w:rank']), (1, 1))
        self.assertEqual((got['a']['w:dense_rank'], got['b']['w:dense_rank']), (1, 1))
        self.assertEqual(got['c']['w:rank'], 3)

    def test_lead_lag_defaults_and_null_keep(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1, 'v': None},
                {'id': 'b', 'g': 1, 'k': 2, 'v': 8},
                {'id': 'c', 'g': 1, 'k': 3, 'v': 9}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': 0, 'end': 0, 'exclude': 'none'}
        fns = [{'window': 'w', 'kind': 'lead', 'field': 'v', 'offset': 2, 'default': -1},
               {'window': 'w', 'kind': 'lag', 'field': 'v', 'offset': 2, 'default': -1},
               {'window': 'w', 'kind': 'lead', 'field': 'v', 'offset': 0, 'default': -1,
                'name': 'self'}]
        got, _ = run(rows, [win], fns)
        self.assertEqual(got['a']['w:lead(v,2)'], 9)
        self.assertEqual(got['b']['w:lead(v,2)'], -1)   # 越界用明确默认值
        self.assertEqual(got['a']['w:lag(v,2)'], -1)
        self.assertIsNone(got['a']['self'])             # 偏移 0 取自身，保留 null

    def test_sum_rejects_non_integer(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1, 'v': 'oops'}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': 0, 'end': 0, 'exclude': 'none'}
        with self.assertRaisesRegex(DataError, '整数'):
            run(rows, [win], [{'window': 'w', 'kind': 'sum', 'field': 'v'}])

    def test_range_rejects_non_integer_key(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1.5, 'v': 1}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'range', 'start': -1, 'end': 1, 'exclude': 'none'}
        with self.assertRaisesRegex(DataError, '整数'):
            run(rows, [win], [{'window': 'w', 'kind': 'count', 'field': 'v'}])


class CrossCheckTests(unittest.TestCase):
    def test_registered_material_matches_reference(self):
        for manifest in ('data/normal.json', 'data/boundary.json'):
            tables = load(manifest)
            fns = demo_fns('w1') + demo_fns('w2')
            fast, stats = collect(run_window_query(
                tables['rows'], tables['windows'], fns, budget_rows=4))
            slow = reference_run(tables['rows'], tables['windows'], fns)
            self.assertEqual(fast, slow, manifest)
            self.assertEqual(stats['sort_groups'], 2)

    def test_input_shard_order_is_irrelevant(self):
        rng = random.Random(11)
        base = [{'id': f'r{i:02d}', 'tenant': rng.choice(['A', 'B']),
                 'time': rng.choice([None, 1, 2, 2, 3]),
                 'cost': rng.choice([None, 1, 5, 9])} for i in range(40)]
        windows = [{'id': 'w', 'partition': ['tenant'],
                    'order': [{'field': 'time', 'direction': 'desc', 'nulls': 'first'}],
                    'unit': 'groups', 'start': -2, 'end': 1, 'exclude': 'none'}]
        fns = demo_fns('w')
        want, _ = run(list(base), windows, fns)
        for _ in range(5):
            shuffled = list(base)
            rng.shuffle(shuffled)
            got, _ = run(shuffled, windows, fns)
            self.assertEqual(got, want)

    def test_randomized_small_cases_match_reference(self):
        rng = random.Random(20261114)
        for case in range(150):
            nrows = rng.randint(0, 22)
            rows = [{'id': f'r{i:03d}',
                     'tenant': rng.choice([None, 'A', 'B', 'C']),
                     'time': rng.choice([None, None, 0, 1, 2, 5, 10]),
                     'cost': rng.choice([None, 0, 1, 2, 7, 20])}
                    for i in range(nrows)]
            windows = []
            for j in range(rng.randint(1, 3)):
                unit = rng.choice(['rows', 'groups', 'range'])
                if unit == 'range':
                    order = [{'field': rng.choice(['time', 'cost']),
                              'direction': rng.choice(['asc', 'desc']),
                              'nulls': rng.choice(['first', 'last'])}]
                else:
                    order = [{'field': rng.choice(['time', 'cost']),
                              'direction': rng.choice(['asc', 'desc']),
                              'nulls': rng.choice(['first', 'last'])}]
                    if rng.random() < 0.4:
                        order.append({'field': 'cost' if order[0]['field'] == 'time' else 'time',
                                      'direction': rng.choice(['asc', 'desc']),
                                      'nulls': rng.choice(['first', 'last'])})
                start = rng.randint(-3, 2)
                windows.append({'id': f'w{j}',
                                'partition': rng.choice([[], ['tenant']]),
                                'order': order, 'unit': unit,
                                'start': start, 'end': start + rng.randint(0, 3),
                                'exclude': rng.choice(['none', 'current', 'group', 'ties'])})
            fns = []
            for w in windows:
                for kind in rng.sample(['count', 'sum', 'avg', 'min', 'max'], 3):
                    fns.append({'window': w['id'], 'kind': kind, 'field': 'cost'})
                fns.append({'window': w['id'], 'kind': rng.choice(
                    ['row_number', 'rank', 'dense_rank'])})
                nav = rng.choice(['lead', 'lag'])
                fns.append({'window': w['id'], 'kind': nav, 'field': 'cost',
                            'offset': rng.randint(0, 3),
                            'default': rng.choice([None, -1])})
            budget = rng.choice([4, 8, 64])
            fast, stats = collect(run_window_query(
                list(rows), windows, fns, budget_rows=budget))
            slow = reference_run(list(rows), windows, fns)
            self.assertEqual(fast, slow, f'case {case} budget {budget}')
            self.assertTrue(stats['temp']['cleaned'])


class ResourceTests(unittest.TestCase):
    def _big_rows(self, n, peer_run, seed=7):
        rng = random.Random(seed)
        rows = []
        for i in range(n):
            rows.append({'id': f'r{i:05d}', 'tenant': 'big',
                         'time': 10 if i < peer_run else rng.randint(0, 50),
                         'cost': None if i % 17 == 0 else rng.randint(0, 100)})
        return rows

    def _big_windows(self):
        return [
            {'id': 'cum', 'partition': ['tenant'],
             'order': [{'field': 'time', 'direction': 'asc', 'nulls': 'last'}],
             'unit': 'groups', 'start': -1000000, 'end': 0, 'exclude': 'none'},
            {'id': 'move', 'partition': ['tenant'],
             'order': [{'field': 'time', 'direction': 'asc', 'nulls': 'last'}],
             'unit': 'rows', 'start': -5, 'end': 5, 'exclude': 'none'},
            {'id': 'rng', 'partition': ['tenant'],
             'order': [{'field': 'time', 'direction': 'desc', 'nulls': 'first'}],
             'unit': 'range', 'start': -3, 'end': 2, 'exclude': 'none'},
        ]

    def test_huge_partition_tiny_budget(self):
        # 单个 peer 组（800 行）远超预算（8 行槽），多窗口共享排序
        rows = self._big_rows(1000, 800)
        windows = self._big_windows()
        fns = [{'window': 'cum', 'kind': 'sum', 'field': 'cost'},
               {'window': 'cum', 'kind': 'count', 'field': 'cost'},
               {'window': 'cum', 'kind': 'rank'},
               {'window': 'cum', 'kind': 'dense_rank'},
               {'window': 'cum', 'kind': 'lead', 'field': 'cost',
                'offset': 50, 'default': -1},
               {'window': 'move', 'kind': 'avg', 'field': 'cost'},
               {'window': 'move', 'kind': 'count', 'field': 'cost'},
               {'window': 'rng', 'kind': 'sum', 'field': 'cost'},
               {'window': 'rng', 'kind': 'min', 'field': 'cost'},
               {'window': 'rng', 'kind': 'max', 'field': 'cost'}]
        budget = Budget(8)
        fast, stats = collect(run_window_query(list(rows), windows, fns, budget_rows=8))
        slow = reference_run(list(rows), windows, fns)
        self.assertEqual(fast, slow)
        self.assertEqual(stats['sort_groups'], 2)
        self.assertGreater(stats['spill']['segments'], 0)
        self.assertGreater(stats['spill']['bytes_written'], 0)
        self.assertGreater(stats['spill']['bytes_read'], 0)
        self.assertLessEqual(stats['memory']['peak_rows'],
                             budget.bound(minmax_functions=2))
        self.assertLessEqual(stats['spill']['peak_open_files'], budget.fan_in + 4)
        self.assertTrue(stats['temp']['cleaned'])
        self.assertFalse(os.path.exists(stats['temp']['dir']))

    def test_minmax_deque_overflow_falls_back_to_recompute(self):
        # 升序键 + min 累计帧：候选队列持续增长，超出预算后回退重算
        rows = [{'id': f'r{i:04d}', 'g': 1, 'k': i, 'v': i} for i in range(400)]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': -1000000, 'end': 0, 'exclude': 'none'}
        fns = [{'window': 'w', 'kind': 'min', 'field': 'v'},
               {'window': 'w', 'kind': 'max', 'field': 'v'}]
        fast, stats = collect(run_window_query(rows, [win], fns, budget_rows=8))
        slow = reference_run(rows, [win], fns)
        self.assertEqual(fast, slow)
        self.assertIn('w:min(v)', stats['eval']['fallbacks'])

    def test_exclusion_uses_recompute_strategy(self):
        rng = random.Random(3)
        rows = [{'id': f'r{i:03d}', 'g': 1,
                 'k': rng.choice([None, 1, 1, 2, 3]),
                 'v': rng.choice([None, 1, 2, 3])} for i in range(300)]
        windows = [
            {'id': 'wg', 'partition': ['g'],
             'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
             'unit': 'groups', 'start': -2, 'end': 1, 'exclude': 'group'},
            {'id': 'wt', 'partition': ['g'],
             'order': [{'field': 'k', 'direction': 'desc', 'nulls': 'first'}],
             'unit': 'range', 'start': -1, 'end': 1, 'exclude': 'ties'},
            {'id': 'wc', 'partition': ['g'],
             'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
             'unit': 'rows', 'start': -3, 'end': 3, 'exclude': 'current'},
        ]
        fns = []
        for w in windows:
            fns += [{'window': w['id'], 'kind': k, 'field': 'v'}
                    for k in ('count', 'sum', 'avg', 'min', 'max')]
        fast, stats = collect(run_window_query(rows, windows, fns, budget_rows=8))
        slow = reference_run(rows, windows, fns)
        self.assertEqual(fast, slow)
        self.assertEqual(len(stats['eval']['recompute']), len(fns))
        self.assertEqual(stats['eval']['sliding'], [])

    def test_lead_lag_across_spill_blocks(self):
        rng = random.Random(5)
        rows = [{'id': f'r{i:03d}', 'g': 1, 'k': i,
                 'v': None if i % 13 == 0 else rng.randint(0, 50)}
                for i in range(500)]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': 0, 'end': 0, 'exclude': 'none'}
        fns = [{'window': 'w', 'kind': 'lead', 'field': 'v', 'offset': 37, 'default': -1},
               {'window': 'w', 'kind': 'lag', 'field': 'v', 'offset': 19, 'default': -1}]
        fast, stats = collect(run_window_query(rows, [win], fns, budget_rows=4))
        slow = reference_run(rows, [win], fns)
        self.assertEqual(fast, slow)
        self.assertGreater(stats['spill']['segments'], 0)

    def test_corrupted_segment_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            spill = SpillManager(d)
            ref = spill.write_segment([{'a': 1}, {'a': 2}], 't')
            with open(ref.path, 'r+b') as fh:
                fh.seek(2)
                original = fh.read(1)
                fh.seek(2)
                fh.write(bytes([original[0] ^ 0x01]))
            with self.assertRaisesRegex(DataError, '摘要'):
                spill.read_segment(ref)
            spill.close()

    def test_truncated_segment_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            spill = SpillManager(d)
            ref = spill.write_segment([{'a': 1}, {'a': 2}], 't')
            with open(ref.path, 'r+b') as fh:
                fh.truncate(4)
            with self.assertRaisesRegex(DataError, '损坏|摘要'):
                spill.read_segment(ref)
            spill.close()

    def test_corrupted_sort_run_fails_query(self):
        rows = [{'id': f'r{i:03d}', 'g': 1, 'k': i, 'v': i} for i in range(40)]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': -1, 'end': 1, 'exclude': 'none'}
        fns = [{'window': 'w', 'kind': 'sum', 'field': 'v'}]
        result = run_window_query(rows, [win], fns, budget_rows=4)
        # 第二个溢写段落盘后立即破坏，后续读取必须失败
        spill = result.spill
        original = spill.write_segment
        written = []

        def corrupting_write(items, tag='seg'):
            ref = original(items, tag)
            written.append(ref)
            if len(written) == 2:
                with open(ref.path, 'r+b') as fh:
                    byte = fh.read(1)
                    fh.seek(0)
                    fh.write(bytes([byte[0] ^ 0xFF]))
            return ref

        spill.write_segment = corrupting_write
        try:
            with self.assertRaisesRegex(DataError, '摘要|损坏'):
                collect(result)
        finally:
            result.close()
        self.assertGreaterEqual(len(written), 2)
        self.assertTrue(result.stats['temp']['cleaned'])

    def test_early_termination_cleans_temp_resources(self):
        rows = self._big_rows(2000, 500)
        windows = self._big_windows()
        fns = [{'window': 'cum', 'kind': 'sum', 'field': 'cost'},
               {'window': 'rng', 'kind': 'sum', 'field': 'cost'}]
        result = run_window_query(iter(rows), windows, fns, budget_rows=4)
        spill_dir = result.stats['temp']['dir']
        first = next(iter(result))
        self.assertIn('id', first)
        self.assertTrue(os.path.isdir(spill_dir))
        result.close()
        self.assertTrue(result.stats['temp']['cleaned'])
        self.assertFalse(os.path.exists(spill_dir))
        with self.assertRaises(StopIteration):
            next(result)

    def test_context_manager_cleans_up(self):
        rows = self._big_rows(300, 50)
        windows = self._big_windows()
        fns = [{'window': 'cum', 'kind': 'sum', 'field': 'cost'}]
        with run_window_query(rows, windows, fns, budget_rows=4) as result:
            spill_dir = result.stats['temp']['dir']
            next(iter(result))
        self.assertFalse(os.path.exists(spill_dir))

    def test_empty_input(self):
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': -1, 'end': 1, 'exclude': 'none'}
        out, stats = collect(run_window_query([], [win], demo_fns('w', 'v')))
        self.assertEqual(out, [])
        self.assertTrue(stats['temp']['cleaned'])

    def test_duplicate_row_id_rejected(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1, 'v': 1},
                {'id': 'a', 'g': 1, 'k': 2, 'v': 2}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': -1, 'end': 1, 'exclude': 'none'}
        with self.assertRaisesRegex(DataError, '行id重复'):
            run(rows, [win], [{'window': 'w', 'kind': 'sum', 'field': 'v'}])

    def test_missing_field_rejected(self):
        rows = [{'id': 'a', 'g': 1, 'k': 1}]
        win = {'id': 'w', 'partition': ['g'],
               'order': [{'field': 'k', 'direction': 'asc', 'nulls': 'last'}],
               'unit': 'rows', 'start': -1, 'end': 1, 'exclude': 'none'}
        with self.assertRaisesRegex(DataError, '缺少字段'):
            run(rows, [win], [{'window': 'w', 'kind': 'sum', 'field': 'v'}])

    def test_budget_validation(self):
        with self.assertRaisesRegex(DataError, '预算'):
            Budget(3)
        with self.assertRaisesRegex(DataError, '预算'):
            Budget('8')


class CliTests(unittest.TestCase):
    def _run_cli(self, argv):
        buf = text_io.StringIO()
        with redirect_stdout(buf):
            cli.main(argv)
        return json.loads(buf.getvalue())

    def test_check_mode_unchanged(self):
        report = self._run_cli(['data/normal.json'])
        self.assertEqual(set(report), {'tables', 'identity_checked'})
        self.assertEqual(report['tables'], {'rows': 4, 'windows': 2})

    def test_query_mode(self):
        report = self._run_cli(['--query', '--budget-rows', '8', 'data/boundary.json'])
        self.assertIn('check', report)
        query = report['query']
        ids = [row['id'] for row in query['results']]
        self.assertEqual(ids, sorted(ids))
        self.assertTrue(query['stats']['temp']['cleaned'])
        self.assertGreater(query['stats']['spill']['segments'], 0)

    def test_query_mode_bad_manifest_exit_code(self):
        with self.assertRaises(SystemExit) as ctx:
            self._run_cli(['--query', 'data/broken-digest.json'])
        self.assertEqual(ctx.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
