import argparse
import asyncio

from .tat import TAT


def main():
    arg_parser = argparse.ArgumentParser(conflict_handler='resolve')
    arg_parser.add_argument('-t', '--table_name', required=True)
    arg_parser.add_argument('-c', '--column', action='append', help='column:new_type', default=[])
    arg_parser.add_argument('-h', '--host')
    arg_parser.add_argument('-p', '--port')
    arg_parser.add_argument('-d', '--dbname')
    arg_parser.add_argument('-U', '--user')
    arg_parser.add_argument('-W', '--password')
    arg_parser.add_argument('-p', '--port')
    arg_parser.add_argument('-j', '--jobs', type=int, default=1)
    arg_parser.add_argument('--force', action='store_true')
    arg_parser.add_argument('--cleanup', action='store_true')
    arg_parser.add_argument('--lock-timeout', type=int, default=5)
    arg_parser.add_argument('--time-between-locks', type=int, default=10)
    arg_parser.add_argument('--work-mem', type=str, default='1GB')
    arg_parser.add_argument('--min-delta-rows', type=int, default=10000)
    arg_parser.add_argument('--skip-fk-validation', action='store_true')
    arg_parser.add_argument('--show-queries', action='store_true')
    arg_parser.add_argument('--batch-size', type=int, default=0)
    args = arg_parser.parse_args()

    t = TAT(args)
    asyncio.run(t.run())
