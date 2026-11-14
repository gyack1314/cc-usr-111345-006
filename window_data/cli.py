"""检查资料的完整性并列出业务记录数量；可选在登记窗口上执行演示查询。"""
import argparse
import json
from .io import load, DataError
from .domain import inspect_tables


def demo_functions(windows):
    """对每个登记窗口生成一组演示函数：聚合、排名与 lead/lag。"""
    functions = []
    for window in windows:
        wid = window['id']
        for kind in ('count', 'sum', 'avg', 'min', 'max'):
            functions.append({'window': wid, 'kind': kind, 'field': 'cost'})
        for kind in ('row_number', 'rank', 'dense_rank'):
            functions.append({'window': wid, 'kind': kind})
        functions.append({'window': wid, 'kind': 'lead', 'field': 'cost',
                          'offset': 1, 'default': None})
        functions.append({'window': wid, 'kind': 'lag', 'field': 'cost',
                          'offset': 1, 'default': None})
    return functions


def main(argv=None):
    parser = argparse.ArgumentParser(description='检查业务资料的身份与完整性')
    parser.add_argument('manifest')
    parser.add_argument('--query', action='store_true',
                        help='在登记的窗口定义上执行演示窗口查询')
    parser.add_argument('--budget-rows', type=int, default=1024,
                        help='窗口执行的内存预算（行槽数）')
    parser.add_argument('--spill-dir', default=None,
                        help='溢写临时目录的父目录（默认系统临时目录）')
    args = parser.parse_args(argv)
    try:
        tables = load(args.manifest)
        check = inspect_tables(tables)
        if not args.query:
            print(json.dumps(check, ensure_ascii=False, indent=2))
            return
        from .api import collect, run_window_query
        result = run_window_query(
            tables['rows'], tables['windows'], demo_functions(tables['windows']),
            budget_rows=args.budget_rows, spill_dir=args.spill_dir)
        rows, stats = collect(result)
        report = {'check': check, 'query': {'results': rows, 'stats': stats}}
        print(json.dumps(report, ensure_ascii=False, indent=2))
    except (DataError, ValueError) as exc:
        parser.exit(2, str(exc) + '\n')


if __name__ == '__main__':
    main()
