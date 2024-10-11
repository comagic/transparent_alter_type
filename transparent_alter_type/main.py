import argparse
import psycopg2
from select import select
import psycopg2.extras
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE
from .tat import TAT


def wait_select_inter(conn):
    while 1:
        try:
            state = conn.poll()
            if state == POLL_OK:
                break
            elif state == POLL_READ:
                select([conn.fileno()], [], [])
            elif state == POLL_WRITE:
                select([], [conn.fileno()], [])
            else:
                raise conn.OperationalError(
                    "bad state from poll: %s" % state)
        except KeyboardInterrupt:
            conn.cancel()
            # the loop will be broken by a server error
            continue


def main():
    arg_parser = argparse.ArgumentParser(conflict_handler='resolve')
    arg_parser.add_argument('-h', '--host', required=True)
    arg_parser.add_argument('-p', '--port', type=int, required=True)
    arg_parser.add_argument('-d', '--dbname', required=True)
    arg_parser.add_argument('-t', '--table_name', required=True)
    arg_parser.add_argument('-c', '--column', action='append', help='column:new_type', default=[])
    arg_parser.add_argument('-j', '--jobs', type=int, required=True)
    arg_parser.add_argument('--force', action='store_true')
    arg_parser.add_argument('--cleanup', action='store_true')
    arg_parser.add_argument('--lock-timeout', type=int, default=5)
    arg_parser.add_argument('--time-between-locks', type=int, default=10)
    arg_parser.add_argument('--work-mem', type=str, default='1GB')
    arg_parser.add_argument('--min-delta-rows', type=int, default=10000)
    arg_parser.add_argument('--show-queries', action='store_true')
    arg_parser.add_argument('--skip-fk-validation', action='store_true')
    arg_parser.add_argument('--pgbouncer-host')
    arg_parser.add_argument('--pgbouncer-port', type=int)
    arg_parser.add_argument('--pgbouncer-pause-timeout', type=int, default=2)
    arg_parser.add_argument('--pgbouncer-time-between-pause', type=int, default=10)
    args = arg_parser.parse_args()

    psycopg2.extensions.set_wait_callback(wait_select_inter)

    t = TAT(args)
    t.run()
