import os
import sys
import re
import psycopg2
import psycopg2.extras
import threading
import queue
import time
import datetime
from pg_export.acl import acl_to_grants


class TAT:
    def __init__(self, args):
        self.host = args.host
        self.port = args.port
        self.dbname = args.dbname
        self.args_table_name = args.table_name
        self.table_name = None
        self.jobs = args.jobs
        self.lock_timeout = f'{args.lock_timeout}s'
        self.time_between_locks = args.time_between_locks
        self.work_mem = args.work_mem
        self.is_force = args.force
        self.is_cleanup = args.cleanup
        self.show_queries = args.show_queries
        self.min_delta_rows = args.min_delta_rows
        self.is_skip_fk_validation = args.skip_fk_validation
        self.pgbouncer_host = args.pgbouncer_host
        self.pgbouncer_port = args.pgbouncer_port
        self.pgbouncer_pause_timeout = args.pgbouncer_pause_timeout
        self.pgbouncer_time_between_pause = args.pgbouncer_time_between_pause
        self.columns = [{'column': c.split(':')[0],
                         'type': c.split(':')[1]}
                        for c in args.column]
        self.pgbouncer_connect = self.autocommit_pgbouncer_connect()
        self.main_connect = self.connect()

    def connect(self):
        con = psycopg2.connect(dbname=self.dbname,
                               host=self.host,
                               port=self.port,
                               application_name='transparent_alter_type',
                               cursor_factory=psycopg2.extras.RealDictCursor)
        c = con.cursor()
        c.execute('set lock_timeout = %s', (self.lock_timeout, ))
        c.execute('set work_mem = %s', (self.work_mem, ))
        c.execute('set maintenance_work_mem = %s', (self.work_mem, ))
        con.commit()
        c.close()
        return con

    def autocommit_pgbouncer_connect(self):
        if not self.pgbouncer_host or not self.pgbouncer_port:
            return None
        con = psycopg2.connect(dbname='pgbouncer',
                               host=self.pgbouncer_host,
                               port=self.pgbouncer_port)
        con.autocommit = True
        return con

    def start_transaction(self):
        self.main_cursor = self.main_connect.cursor()

    def execute(self, query, *params):
        if self.show_queries:
            print('\nQUERY: ', query)
        if not query:
            return
        if not params:
            self.main_cursor.execute(query)
        else:
            self.main_cursor.execute(query, params)
        if self.main_cursor.description:
            return self.main_cursor.fetchall()

    def commit(self):
        self.main_connect.commit()
        self.main_cursor.close()

    def rollback(self):
        self.main_connect.rollback()
        self.main_cursor.close()

    def duration(self, interval):
        return str(datetime.timedelta(seconds=int(interval)))

    def cancel_autovacuum(self):
        if self.execute(f'''select pg_cancel_backend(pid)
                              from pg_stat_activity
                             where state = 'active' and
                                   backend_type = 'autovacuum worker' and
                                   query ~ '{self.table_name}';'''):
            print('autovacuum canceled')

    def cancel_all_autovacuum(self):
        if self.execute('''select pg_cancel_backend(pid)
                             from pg_stat_activity
                            where state = 'active' and
                                  backend_type = 'autovacuum worker';'''):
            print('autovacuum canceled')

    @staticmethod
    def get_query(query_file_name):
        full_file_name = os.path.join(os.path.dirname(__file__), 'queries', query_file_name)
        return open(full_file_name).read()

    def get_table_info(self):
        query = self.get_query('get_table_info.sql')
        self.start_transaction()
        res = self.execute(query, self.args_table_name)

        if not res:
            raise Exception('table not found')

        self.table = res[0]
        self.table_name = self.table['name']

        if not self.table['pk_columns']:
            raise Exception(f'table {self.args_table_name} does not have primary key or not null unique constraint')

        if not self.is_force:
            columns_to_alter = []
            for column in self.columns:
                pg_type = self.execute('select %s::regtype as mnemonic', column['type'])[0]['mnemonic']
                if self.table['column_types'][column['column']] == pg_type:
                    print(f'column {self.args_table_name}.{column["column"]} has already type {pg_type}')
                else:
                    columns_to_alter.append(column)
            if len(columns_to_alter) == 0:
                print('no column to alter, use --force to alter anyway')
                sys.exit(0)
            columns = columns_to_alter  # FIXME

    def create_table_new(self):
        self.start_transaction()

        print('{name} ({pretty_size}):'.format(**self.table))
        print(f'  create {self.table_name}__tat_new')
        sys.stdout.flush()

        self.execute(f'''
            create table {self.table_name}__tat_new(
              like {self.table_name}
              including all
              excluding indexes
              excluding constraints
              excluding statistics);
        ''')

        if self.columns:
            self.execute(
                ''.join(
                    '''
                    alter table {name}__tat_new
                      alter column {column}
                        type {type} using ({column}::{type});
                    '''.format(**self.table, **column)
                    for column in self.columns
                )
            )

        self.execute('\n'.join(self.table['create_check_constraints']))
        self.execute('\n'.join(self.table['grant_privileges']))
        self.execute(self.table['comment'])
        self.cancel_autovacuum()
        self.execute(f'''
            alter table {self.table_name}          set (autovacuum_enabled = false);
            alter table {self.table_name}__tat_new set (autovacuum_enabled = false);
        ''')

        self.commit()

    def create_table_delta(self):
        print(f'  create {self.table_name}__tat_delta')
        sys.stdout.flush()
        self.start_transaction()

        self.execute(f'''
            create unlogged table {self.table_name}__tat_delta(
              like {self.table_name} excluding all)
        ''')

        self.execute(f'''
            alter table {self.table_name}__tat_delta add column tat_delta_id serial;
            alter table {self.table_name}__tat_delta add column tat_delta_op "char";
        ''')

        function_body = self.get_query('store_delta.plpgsql')
        self.execute(function_body.format(**self.table))

        columns = ', '.join(
            f'"{column}"'
            for column in self.table['all_columns']
        )
        val_columns = ', '.join(
            f'r."{column}"'
            for column in self.table['all_columns']
        )
        where = ' and '.join(
            f't."{column}" = r."{column}"'
            for column in self.table['pk_columns']
        )
        set_columns = ','.join(
            f'"{column}" = r."{column}"'
            for column in self.table['all_columns']
            if column not in self.table['pk_columns']
        )

        function_body = self.get_query('apply_delta.plpgsql')
        self.execute(function_body.format(**self.table, **locals()))

        self.cancel_autovacuum()
        self.execute(f'''
            create trigger store__tat_delta
              after insert or delete or update on {self.table_name}
              for each row execute procedure "{self.table_name}__store_delta"();
        ''')
        self.commit()

    def copy_data(self):
        start_time = time.time()
        print(f'  copy data ({self.table["pretty_data_size"]})... ', end='')
        sys.stdout.flush()
        self.start_transaction()
        self.execute(f'''
            insert into {self.table_name}__tat_new
              select * from {self.table_name}
        ''')
        self.commit()
        print('done in', self.duration(time.time() - start_time))
        sys.stdout.flush()

    def create_indexes(self):
        start_time = time.time()
        index_count = len(self.table['create_indexes'])
        print(f'  create {index_count} indexes on {self.jobs} jobs:')
        if not self.table['create_indexes']:
            print('    no indexes')
            return

        self.exception_on_create_index = False
        self.output_queue = queue.Queue()

        self.workers = [threading.Thread(target=self.create_index) for i in range(self.jobs)]
        for w in self.workers:
            w.start()

        while threading.active_count() > 1:
            time.sleep(0.5)
            if not self.output_queue.empty():
                print(self.output_queue.get())
        while not self.output_queue.empty():
            print(self.output_queue.get())

        if self.exception_on_create_index:
            raise Exception('exception on create index')
        print('  create_indexes done in', self.duration(time.time() - start_time))

    def get_next_index(self):
        try:
            return self.table['create_indexes'].pop()
        except IndexError:
            return None

    def create_index(self):
        try:
            connect = self.connect()
            while True:
                start_time = time.time()
                index_def = self.get_next_index()
                if not index_def:
                    break
                index_name = re.sub('CREATE U?N?I?Q?U?E? ?INDEX (.*) ON .*', '\\1', index_def)
                self.output_queue.put(f'    start {index_name}')
                cursor = connect.cursor()
                cursor.execute(index_def)
                connect.commit()
                cursor.close()
                duration = self.duration(time.time() - start_time)
                self.output_queue.put(f'    done {index_name} in {duration}')
        except Exception as e:
            self.exception_on_create_index = True
            raise e

    def apply_delta(self):
        start_time = time.time()
        print('    apply_delta... ', end='')
        sys.stdout.flush()
        rows = self.execute(f'select "{self.table_name}__apply_delta"() as rows;')[0]['rows']
        print(rows, 'rows done in', self.duration(time.time() - start_time))
        return rows

    def analyze(self):
        start_time = time.time()
        print('  analyze... ', end='')
        sys.stdout.flush()

        self.start_transaction()
        self.execute(f'analyze {self.table_name}__tat_new')
        self.commit()
        print('done in', self.duration(time.time() - start_time))

    def exclusive_lock_table(self):
        self.start_transaction()
        self.cancel_autovacuum()
        print(f'    lock table {self.table_name}... ', end='')
        sys.stdout.flush()
        try:
            self.execute(f'lock table {self.table_name} in access exclusive mode')
        except (psycopg2.errors.LockNotAvailable, psycopg2.errors.DeadlockDetected) as e:
            self.rollback()
            print('failed:', e)
            return False
        print('done')
        return True

    def restore_storage_parameters(self):
        self.execute(f'alter table {self.table_name} reset (autovacuum_enabled);')
        self.execute('\n'.join(self.table['storage_parameters']))

    def switch_table(self):
        print('  switch table start:')

        while True:
            self.start_transaction()
            rows = self.apply_delta()
            self.commit()
            if rows <= self.min_delta_rows:
                break

        while True:
            if self.pgbouncer_pause():
                if self.exclusive_lock_table():
                    break
                else:
                    self.pgbouncer_resume()
                    time.sleep(self.time_between_locks)
            else:
                time.sleep(self.pgbouncer_time_between_pause)

            self.start_transaction()
            rows = self.apply_delta()
            self.commit()

        try:
            self.apply_delta()
            self.execute('\n'.join(self.table['drop_functions']))
            self.cancel_all_autovacuum()
            self.execute('\n'.join(self.table['drop_views']))
            self.execute('\n'.join(self.table['drop_constraints']))
            self.execute('\n'.join(self.table['alter_sequences']))
            print(f'    drop table {self.table_name}')
            self.execute(f'drop table {self.table_name};')
            self.execute(f'drop function "{self.table_name}__store_delta"();')
            self.execute(f'drop function "{self.table_name}__apply_delta"();')
            self.execute(f'drop table {self.table_name}__tat_delta;')
            print(f'    rename table {self.table_name}__tat_new -> {self.table_name}')
            self.execute(f'alter table {self.table_name}__tat_new rename to {self.table["name_without_schema"]};')
            self.execute('\n'.join(self.table['rename_indexes']))
            self.execute('\n'.join(self.table['create_constraints']))
            self.execute('\n'.join(self.table['create_triggers']))
            self.execute('\n'.join(self.table['create_views']))
            self.execute('\n'.join([acl_to_grants(params['acl'],
                                                  params['obj_type'],
                                                  params['obj_name'])
                                    for params in self.table['view_acl_to_grants_params']]))
            self.execute('\n'.join(self.table['comment_views']))
            self.execute('\n'.join(self.table['create_functions']))
            self.execute('\n'.join([acl_to_grants(params['acl'],
                                                  params['obj_type'],
                                                  params['obj_name'])
                                    for params in self.table['function_acl_to_grants_params']]))
            self.restore_storage_parameters()
            self.commit()
            self.pgbouncer_resume()
        except Exception as e:
            self.pgbouncer_resume()
            raise e
        print('  switch table done')

    def validate_constraints(self):
        if not self.table['validate_constraints']:
            return
        start_time = time.time()
        constraints_count = len(self.table["validate_constraints"])
        print(f'  validate {constraints_count} constraints:')
        for c in self.table['validate_constraints']:
            loop_start_time = time.time()
            print('   ', re.sub('alter table (.*) validate constraint (.*);', '\\1: \\2', c), '...', end='')
            sys.stdout.flush()
            self.start_transaction()
            self.execute(c)
            self.commit()
            print('done in', self.duration(time.time() - loop_start_time))
        print('  validate constraints done in', self.duration(time.time() - start_time))

    def break_pgbouncer_pause(self):
        start_time = time.time()
        while self.break_pause_loop and time.time() - start_time < self.pgbouncer_pause_timeout:
            time.sleep(.01)
        if self.break_pause_loop:
            print('    pgbouncer pause timeout: cancel pause')
            self.pause_canceled = True
            self.pgbouncer_connect.cancel()

    def pgbouncer_pause(self):
        if not self.pgbouncer_connect:
            return True
        print('    try pgbouncer pause')
        self.break_pause_loop = True
        self.pause_canceled = False

        t = threading.Thread(target=self.break_pgbouncer_pause)
        t.start()

        try:
            self.pgbouncer_connect.cursor().execute('pause')
        except psycopg2.DatabaseError as e:
            print(f'    pause failed: {e.__class__.__name__}: {str(e)}')
            if self.pause_canceled:
                print('    reconnect to pgbouncer')
                self.pgbouncer_connect.close()
                self.pgbouncer_connect = self.autocommit_pgbouncer_connect()
            if str(e) == 'already suspended/paused\n':
                print('    pgbouncer paused !!!')
                return True
            return False
        finally:
            self.break_pause_loop = False
            t.join()
        print('    pgbouncer paused !!!')
        return True

    def pgbouncer_resume(self):
        if not self.pgbouncer_connect:
            return
        print('    pgbouncer resume')
        try:
            self.pgbouncer_connect.cursor().execute('resume')
        except psycopg2.DatabaseError as e:
            print(f'resume failed: {e.__class__.__name__}: {str(e)}')

    def cleanup(self):
        self.start_transaction()
        self.execute(f'drop trigger if exists store__tat_delta on {self.table_name};')
        self.execute(f'drop function if exists "{self.table_name}__store_delta"();')
        self.execute(f'drop function if exists "{self.table_name}__apply_delta"();')
        self.execute(f'drop table if exists {self.table_name}__tat_delta;')
        self.execute(f'drop table if exists {self.table_name}__tat_new;')
        self.commit()

    def run(self):
        start_time = time.time()
        self.get_table_info()

        if self.is_cleanup:
            self.cancel_autovacuum()
            self.cleanup()
            return

        try:
            self.create_table_new()
            self.create_table_delta()
            self.copy_data()
            self.create_indexes()
            self.analyze()
            self.switch_table()
        except:
            self.rollback()
            self.start_transaction()
            self.cancel_autovacuum()
            self.restore_storage_parameters()
            self.commit()
            raise

        if not self.is_skip_fk_validation:
            self.validate_constraints()

        print(self.table['name'], 'done in', self.duration(time.time() - start_time))
        print()
