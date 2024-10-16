import asyncio
import os
import sys
import re
from contextlib import asynccontextmanager
import time
import datetime

import asyncpg
from pg_export.acl import acl_to_grants

from .pg_pool import PgPool


class TAT:
    def __init__(self, args):
        self.args = args
        self.args_table_name = args.table_name
        self.table_name = None
        self.table = None
        self.jobs = args.jobs
        self.time_between_locks = args.time_between_locks
        self.work_mem = args.work_mem
        self.is_force = args.force
        self.is_cleanup = args.cleanup
        self.show_queries = args.show_queries
        self.min_delta_rows = args.min_delta_rows
        self.is_skip_fk_validation = args.skip_fk_validation
        self.columns = [{'column': c.split(':')[0],
                         'type': c.split(':')[1]}
                        for c in args.column]
        self.db = PgPool(args)

    @staticmethod
    def duration(interval):
        return str(datetime.timedelta(seconds=int(interval)))

    async def cancel_autovacuum(self, con=None):
        if con is None:
            con = self.db
        if await con.fetch(f'''
            select pg_cancel_backend(pid)
              from pg_stat_activity
             where state = 'active' and
                   backend_type = 'autovacuum worker' and
                   query ~ '{self.table_name}';
        '''):
            print('autovacuum canceled')

    @staticmethod
    async def cancel_all_autovacuum(con):
        if await con.fetch('''
            select pg_cancel_backend(pid)
              from pg_stat_activity
             where state = 'active' and
                   backend_type = 'autovacuum worker';
        '''):
            print('autovacuum canceled')

    @staticmethod
    def get_query(query_file_name):
        full_file_name = os.path.join(os.path.dirname(__file__), 'queries', query_file_name)
        return open(full_file_name).read()

    async def get_table_info(self):
        query = self.get_query('get_table_info.sql')
        self.table = await self.db.fetchrow(query, self.args_table_name)

        if not self.table:
            raise Exception('table not found')
        elif not self.table['pk_columns']:
            raise Exception(f'table {self.args_table_name} does not have primary key or not null unique constraint')

        self.table_name = self.table['name']

        if not self.is_force:
            columns_to_alter = []
            for column in self.columns:
                pg_type = await self.db.fetchval('select $1::regtype', column['type'])
                if self.table['column_types'][column['column']] == pg_type:
                    print(f'NOTICE: column {self.args_table_name}.{column["column"]} already has {pg_type} type')
                else:
                    columns_to_alter.append(column)
            if len(columns_to_alter) == 0:
                print('no column to alter, use --force to alter anyway')
                sys.exit(0)
            self.columns = columns_to_alter

    async def create_table_new(self):
        print('{name} ({pretty_size}):'.format(**self.table))
        print(f'  create {self.table_name}__tat_new')

        await self.db.execute(f'''
            create table {self.table_name}__tat_new(
              like {self.table_name}
              including all
              excluding indexes
              excluding constraints
              excluding statistics
            );
        ''')

        if self.columns:
            await self.db.execute(
                ''.join(
                    '''
                    alter table {name}__tat_new
                      alter column {column}
                        type {type} using ({column}::{type});
                    '''.format(**self.table, **column)
                    for column in self.columns
                )
            )

        await self.db.execute('\n'.join(self.table['create_check_constraints']))
        await self.db.execute('\n'.join(self.table['grant_privileges']))
        await self.db.execute(self.table['comment'])
        await self.cancel_autovacuum()
        await self.db.execute(f'''
            alter table {self.table_name}          set (autovacuum_enabled = false);
            alter table {self.table_name}__tat_new set (autovacuum_enabled = false);
        ''')

    async def create_table_delta(self):
        print(f'  create {self.table_name}__tat_delta')

        await self.db.execute(f'''
            create unlogged table {self.table_name}__tat_delta(
              like {self.table_name} excluding all)
        ''')

        await self.db.execute(f'''
            alter table {self.table_name}__tat_delta add column tat_delta_id serial;
            alter table {self.table_name}__tat_delta add column tat_delta_op "char";
        ''')

        query = self.get_query('store_delta.plpgsql')
        await self.db.execute(query.format(**self.table))

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

        query = self.get_query('apply_delta.plpgsql')
        await self.db.execute(query.format(**self.table, **locals()))

        await self.cancel_autovacuum()
        await self.db.execute(f'''
            create trigger store__tat_delta
              after insert or delete or update on {self.table_name}
              for each row execute procedure "{self.table_name}__store_delta"();
        ''')

    async def copy_data(self):
        start_time = time.time()
        print(f'  copy data ({self.table["pretty_data_size"]})... ', end='')
        sys.stdout.flush()
        await self.db.execute(f'''
            insert into {self.table_name}__tat_new
              select * from {self.table_name}
        ''')
        print('done in', self.duration(time.time() - start_time))

    async def create_indexes(self):
        start_time = time.time()
        if not self.table['create_indexes']:
            return
        index_count = len(self.table['create_indexes'])
        print(f'  create {index_count} indexes on {self.args.jobs} jobs:')
        workers = [self.create_index() for _ in range(self.args.jobs)]
        await asyncio.gather(*workers)
        print('  create_indexes done in', self.duration(time.time() - start_time))

    def get_next_index(self):
        try:
            return self.table['create_indexes'].pop()
        except IndexError:
            return None

    async def create_index(self):
        while True:
            start_time = time.time()
            index_def = self.get_next_index()
            if not index_def:
                break
            index_name = re.sub('CREATE U?N?I?Q?U?E? ?INDEX (.*) ON .*', '\\1', index_def)
            print(f'    start {index_name}')
            await self.db.execute(index_def)
            duration = self.duration(time.time() - start_time)
            print(f'    done {index_name} in {duration}')

    async def apply_delta(self, con=None):
        start_time = time.time()
        print('    apply_delta... ', end='')
        sys.stdout.flush()
        if con is None:
            con = self.db
        rows = await con.fetchval(f'select "{self.table_name}__apply_delta"();')
        print(rows, 'rows done in', self.duration(time.time() - start_time))
        return rows

    async def analyze(self):
        start_time = time.time()
        print('  analyze... ', end='')
        sys.stdout.flush()
        await self.db.execute(f'analyze {self.table_name}__tat_new')
        print('done in', self.duration(time.time() - start_time))

    @asynccontextmanager
    async def exclusive_lock_table(self):
        while True:
            async with self.db.transaction() as con:
                await self.cancel_autovacuum(con)
                print(f'    lock table {self.table_name}... ', end='')
                sys.stdout.flush()
                try:
                    await con.execute(f'lock table {self.table_name} in access exclusive mode;')
                    print('done')
                    yield con
                    break
                except (
                    asyncpg.exceptions.LockNotAvailableError,
                    asyncpg.exceptions.DeadlockDetectedError
                ) as e:
                    print('failed:', e)
                    await con.execute('rollback;')
            await asyncio.sleep(self.args.time_between_locks)

    async def switch_table(self):
        print('  switch table start:')

        while True:
            rows = await self.apply_delta()
            if rows <= self.min_delta_rows:
                break

        async with self.exclusive_lock_table() as con:
            await self.apply_delta(con)
            await con.execute('\n'.join(self.table['drop_functions']))
            await self.cancel_all_autovacuum(con)
            await con.execute('\n'.join(self.table['drop_views']))
            await con.execute('\n'.join(self.table['drop_constraints']))
            await con.execute('\n'.join(self.table['alter_sequences']))
            print(f'    drop table {self.table_name}')
            await con.execute(f'drop table {self.table_name};')
            await con.execute(f'drop function "{self.table_name}__store_delta"();')
            await con.execute(f'drop function "{self.table_name}__apply_delta"();')
            await con.execute(f'drop table {self.table_name}__tat_delta;')
            print(f'    rename table {self.table_name}__tat_new -> {self.table_name}')
            await con.execute(f'alter table {self.table_name}__tat_new rename to {self.table["name_without_schema"]};')
            await con.execute('\n'.join(self.table['rename_indexes']))
            await con.execute('\n'.join(self.table['create_constraints']))
            await con.execute('\n'.join(self.table['create_triggers']))
            await con.execute('\n'.join(self.table['create_views']))
            await con.execute('\n'.join(
                acl_to_grants(params['acl'],
                              params['obj_type'],
                              params['obj_name'])
                for params in self.table['view_acl_to_grants_params']
            ))
            await con.execute('\n'.join(self.table['comment_views']))
            await con.execute('\n'.join(self.table['create_functions']))
            await con.execute('\n'.join(
                acl_to_grants(params['acl'],
                              params['obj_type'],
                              params['obj_name'])
                for params in self.table['function_acl_to_grants_params']
            ))
            await con.execute(f'alter table {self.table_name} reset (autovacuum_enabled);')
            await con.execute('\n'.join(self.table['storage_parameters']))
        print('  switch table done')

    async def validate_constraints(self):
        if not self.table['validate_constraints']:
            return
        if self.is_skip_fk_validation:
            for constraint in self.table['validate_constraints']:
                print(f'skip constraint validation: {constraint}')
            return
        start_time = time.time()
        constraints_count = len(self.table["validate_constraints"])
        print(f'  validate {constraints_count} constraints:')
        for constraint in self.table['validate_constraints']:
            loop_start_time = time.time()
            constraint_name = re.sub('alter table (.*) validate constraint (.*);', '\\1: \\2', constraint)
            print(f'   {constraint_name}...', end='')
            sys.stdout.flush()
            await self.db.execute(constraint)
            print('done in', self.duration(time.time() - loop_start_time))
        print('  validate constraints done in', self.duration(time.time() - start_time))

    async def cleanup(self):
        await self.db.execute(f'drop trigger if exists store__tat_delta on {self.table_name};')
        await self.db.execute(f'drop function if exists "{self.table_name}__store_delta"();')
        await self.db.execute(f'drop function if exists "{self.table_name}__apply_delta"();')
        await self.db.execute(f'drop table if exists {self.table_name}__tat_delta;')
        await self.db.execute(f'drop table if exists {self.table_name}__tat_new;')

    async def run(self):
        start_time = time.time()
        await self.db.init_pool()
        await self.get_table_info()

        if self.is_cleanup:
            await self.cancel_autovacuum()
            await self.cleanup()
            return

        try:
            await self.create_table_new()
            await self.create_table_delta()
            await self.copy_data()
            await self.create_indexes()
            await self.analyze()
            await self.switch_table()
        except Exception as e:
            await self.cancel_autovacuum()
            await self.db.execute(f'alter table {self.table_name} reset (autovacuum_enabled);')
            raise e

        await self.validate_constraints()

        print(self.table['name'], 'done in', self.duration(time.time() - start_time))
        print()
