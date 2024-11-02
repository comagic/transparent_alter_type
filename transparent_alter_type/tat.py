import asyncio
import os
import sys
import re
from argparse import Namespace
from contextlib import asynccontextmanager
import time
import datetime
from enum import Enum
from typing import List

import asyncpg
from pg_export.acl import acl_to_grants

from .pg_pool import PgPool


class TableKind(Enum):
    regular = 'r'
    foreign = 'f'
    partitioned = 'p'


class TAT:
    children: List["TAT"]
    table_kind: TableKind

    def __init__(self, args, is_sub_table=False, pool=None):
        self.args = args
        self.is_sub_table = is_sub_table
        self.table_name = None
        self.table = None
        self.children = []
        self.columns = [{'column': c.split(':')[0],
                         'type': c.split(':')[1]}
                        for c in args.column]
        self.db = pool or PgPool(args)
        self.table_locked = False

    @staticmethod
    def duration(start_time):
        return str(datetime.timedelta(seconds=int(time.time() - start_time)))

    def log(self, message):
        print(f'{self.table_name}: {message}')

    def log_border(self):
        print('-' * 50)

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
            self.log('autovacuum canceled')

    async def cancel_all_autovacuum(self, con):
        if await con.fetch('''
            select pg_cancel_backend(pid)
              from pg_stat_activity
             where state = 'active' and
                   backend_type = 'autovacuum worker';
        '''):
            self.log('autovacuum canceled')

    @staticmethod
    def get_query(query_file_name):
        full_file_name = os.path.join(os.path.dirname(__file__), 'queries', query_file_name)
        return open(full_file_name).read()

    async def get_table_info(self, table=None):
        children = []
        tables_data = {}
        if table:
            self.table = table
        else:
            db_table_name = await self.db.fetchval('select $1::regclass::text', self.args.table_name)
            children = await self.db.fetchval(
                self.get_query('get_child_tables.sql'),
                db_table_name
            )
            table_names = [db_table_name] + children
            tables_data = {
                table['name']: table
                for table in await self.db.fetch(
                    self.get_query('get_table_info.sql'),
                    table_names
                )
            }
            self.table = tables_data[db_table_name]

        self.table_kind = TableKind(self.table['kind'])
        self.table_name = self.table['name']

        if not self.table['pk_columns'] and self.table_kind == TableKind.regular:
            raise Exception(f'table {self.table_name} does not have primary key or not null unique constraint')

        if not self.args.force:
            columns_to_alter = []
            for column in self.columns:
                pg_type = await self.db.fetchval('select $1::regtype', column['type'])
                if self.table['column_types'][column['column']] == pg_type:
                    print(f'NOTICE: column {self.table_name}.{column["column"]} already has {pg_type} type')
                else:
                    columns_to_alter.append(column)
            if len(columns_to_alter) == 0:
                print('no column to alter, use --force to alter anyway')
                sys.exit(0)
            self.columns = columns_to_alter

        if not self.is_sub_table:  # all children are processing on root level
            self.children = [
                TAT(Namespace(**dict(vars(self.args), table_name=child)), True, self.db)
                for child in children
            ]
            for child in self.children:
                await child.get_table_info(tables_data[child.args.table_name])

    async def create_table_new(self):
        if self.table_kind == TableKind.foreign:
            return
        self.log(f'create {self.table_name}__tat_new')

        await self.db.execute(f'''
            create table {self.table_name}__tat_new(
              like {self.table_name}
              including all
              excluding indexes
              excluding constraints
              excluding statistics
            ){self.table['partition_expr'] or ''};
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
        if self.table_kind == TableKind.regular:
            await self.cancel_autovacuum()
            await self.db.execute(f'''
                alter table {self.table_name}          set (autovacuum_enabled = false);
                alter table {self.table_name}__tat_new set (autovacuum_enabled = false);
            ''')
        if self.table['attach_expr']:
            await self.db.execute(self.table['attach_expr'])
        elif self.table['inherits']:
            await self.db.execute(
                f'alter table {self.table_name}__tat_new inherit {self.table["inherits"][0]}__tat_new'
            )
        for child in self.children:
            await child.create_table_new()

    async def create_table_delta(self):
        if self.table_kind == TableKind.regular:
            self.log(f'create {self.table_name}__tat_delta')

            await self.db.execute(f'''
                create unlogged table {self.table_name}__tat_delta(
                  like {self.table_name} excluding all)
            ''')

            await self.db.execute(f'''
                alter table {self.table_name}__tat_delta
                  add column tat_delta_id serial,
                  add column tat_delta_op "char";
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

        for child in self.children:
            await child.create_table_delta()

    @staticmethod
    async def run_parallel(tasks, worker_count):
        async def worker():
            while tasks:
                task = tasks.pop()
                await task
        workers = [worker() for _ in range(worker_count)]
        await asyncio.gather(*workers)

    async def copy_data(self):
        self.log_border()
        tasks = []
        if self.table_kind == TableKind.regular:
            tasks.append(self.copy_table())
        for child in self.children:
            if child.table_kind == TableKind.regular:
                tasks.append(child.copy_table())
        await self.run_parallel(tasks, self.args.jobs)

    async def copy_table(self):
        ts = time.time()
        self.log(f'copy data: start ({self.table["pretty_data_size"]})')
        await self.db.execute(f'''
            insert into {self.table_name}__tat_new
              select * from only {self.table_name}
        ''')
        self.log(f'copy data: done ({self.table["pretty_data_size"]}) in {self.duration(ts)}')

    async def create_indexes(self):
        ts = time.time()
        tasks = [
            self.create_index(index_def)
            for index_def in self.table['create_indexes']
        ]
        for child in self.children:
            for index_def in child.table['create_indexes']:
                tasks.append(child.create_index(index_def))
        if not tasks:
            return
        self.log_border()
        self.log(f'create indexes: start ({len(tasks)} indexes on {self.args.jobs} jobs)')
        await self.run_parallel(tasks, self.args.jobs)
        self.log(f'create indexes: done in {self.duration(ts)}')

    async def create_index(self, index_def):
        ts = time.time()
        index_name = re.sub('CREATE U?N?I?Q?U?E? ?INDEX (.*) ON .*', '\\1', index_def)
        self.log(f'create index: {index_name}: start')
        await self.db.execute(index_def)
        self.log(f'create index: {index_name}: done in {self.duration(ts)}')

    async def apply_delta(self, con=None):
        rows = 0
        if self.table_kind == TableKind.regular:
            ts = time.time()
            self.log('apply_delta: start')
            if con is None:
                con = self.db
            rows = await con.fetchval(f'select "{self.table_name}__apply_delta"();')
            self.log(f'apply_delta: done: {rows} rows in {self.duration(ts)}')
        for child in self.children:
            rows += await child.apply_delta(con)
        return rows

    async def analyze(self):
        ts = time.time()
        self.log('analyze: start')
        sys.stdout.flush()
        await self.db.execute(f'analyze {self.table_name}__tat_new')
        self.log(f'analyze: done in {self.duration(ts)}')

    @asynccontextmanager
    async def exclusive_lock_table(self):
        while True:
            async with self.db.transaction() as con:
                await self.cancel_autovacuum(con)
                self.log('lock table: start')
                try:
                    await con.execute(f'lock table {self.table_name} in access exclusive mode;')
                    self.log('lock table: done')
                    self.table_locked = True
                    yield con
                    break
                except (
                    asyncpg.exceptions.LockNotAvailableError,
                    asyncpg.exceptions.DeadlockDetectedError
                ) as e:
                    if self.table_locked:
                        await con.execute('rollback;')
                        raise e
                    self.log(f'lock table: failed: {e}')
                    await con.execute('rollback;')
            await asyncio.sleep(self.args.time_between_locks)

    async def drop_depend_objects(self, con):
        await con.execute('\n'.join(self.table['drop_views']))
        await con.execute('\n'.join(self.table['drop_functions']))
        if self.table['drop_constraints']:
            await self.cancel_all_autovacuum(con)
        await con.execute('\n'.join(self.table['drop_constraints']))
        await con.execute('\n'.join(self.table['alter_sequences']))
        for child in self.children:
            await child.drop_depend_objects(con)

    async def detach_foreign_tables(self, con):
        if self.table_kind == TableKind.foreign:
            if self.table['detach_foreign_expr']:  # declarative partitioning
                self.log('detach foreign table')
                await con.execute(self.table['detach_foreign_expr'])
            else:   # old style inherits partitioning
                self.log('no inherit foreign table')
                await con.execute(
                    f'alter table {self.table_name} no inherit {self.table["inherits"][0]}'
                )
        for child in self.children:
            await child.detach_foreign_tables(con)

    async def attach_foreign_tables(self, con):
        if self.table_kind == TableKind.foreign:
            if self.columns:
                await con.execute(
                    ''.join(
                        '''
                        alter table {name}
                          alter column {column}
                            type {type};
                        '''.format(**self.table, **column)
                        for column in self.columns
                    )
                )
            if self.table['attach_foreign_expr']:  # declarative partitioning
                self.log('attach foreign table')
                await con.execute(self.table['attach_foreign_expr'])
            else:  # old style inherits partitioning
                self.log('inherit foreign table')
                await con.execute(
                    f'alter table {self.table_name} inherit {self.table["inherits"][0]}'
                )
        for child in self.children:
            await child.attach_foreign_tables(con)

    async def drop_original_table(self, con):
        self.log('drop original table')
        if self.table_kind == TableKind.regular and self.children:  # old style inherits partitioning
            for child in reversed(self.children):
                if child.table_kind == TableKind.regular:
                    await con.execute(f'drop table {child.table_name};')
        await con.execute(f'drop table {self.table_name};')

    async def rename_table(self, con):
        if self.table_kind == TableKind.foreign:
            return
        self.log(f'rename table {self.table_name}__tat_new -> {self.table_name}')
        await con.execute(f'alter table {self.table_name}__tat_new rename to {self.table["name_without_schema"]};')
        await con.execute('\n'.join(self.table['rename_indexes']))
        await con.execute('\n'.join(self.table['create_constraints']))
        await con.execute('\n'.join(self.table['create_triggers']))
        await con.execute(self.table['replica_identity'])
        await con.execute('\n'.join(self.table['publications']))
        await con.execute(f'alter table {self.table_name} reset (autovacuum_enabled);')
        await con.execute('\n'.join(self.table['storage_parameters']))
        for child in self.children:
            await child.rename_table(con)

    async def recreate_depend_objects(self, con):
        await con.execute('\n'.join(self.table['create_functions']))
        await con.execute('\n'.join(
            acl_to_grants(params['acl'],
                          params['obj_type'],
                          params['obj_name'])
            for params in self.table['function_acl_to_grants_params']
        ))
        await con.execute('\n'.join(self.table['create_views']))
        await con.execute('\n'.join(
            acl_to_grants(params['acl'],
                          params['obj_type'],
                          params['obj_name'])
            for params in self.table['view_acl_to_grants_params']
        ))
        await con.execute('\n'.join(self.table['comment_views']))
        for child in self.children:
            await child.recreate_depend_objects(con)

    async def switch_table(self):
        self.log_border()
        self.log('switch table: start')

        while True:
            rows = await self.apply_delta()
            if rows <= self.args.min_delta_rows:
                break

        async with self.exclusive_lock_table() as con:
            await self.apply_delta(con)
            await self.drop_depend_objects(con)
            await self.cleanup(con, with_tat_new=False)
            await self.detach_foreign_tables(con)
            await self.drop_original_table(con)
            await self.rename_table(con)
            await self.attach_foreign_tables(con)
            await self.recreate_depend_objects(con)
        self.log('switch table: done')

    async def validate_constraints(self):
        for child in self.children:
            self.table['validate_constraints'].extend(child.table['validate_constraints'])
        if not self.table['validate_constraints']:
            return
        self.log_border()
        if self.args.skip_fk_validation:
            for constraint in self.table['validate_constraints']:
                self.log(f'skip constraint validation: {constraint}')
            return
        ts = time.time()
        constraints_count = len(self.table["validate_constraints"])
        self.log(f'validate constraints: start ({constraints_count})')
        for constraint in self.table['validate_constraints']:
            loop_ts = time.time()
            constraint_name = re.sub('alter table (.*) validate constraint (.*);', '\\1: \\2', constraint)
            self.log(f'validate constraint: {constraint_name}: start')
            await self.db.execute(constraint)
            self.log(f'validate constraint: {constraint_name}: done in {self.duration(loop_ts)}')
        self.log(f'validate constraints: done in {self.duration(ts)}')

    async def cleanup(self, db=None, with_tat_new=True):
        if not db:
            db = self.db
        for child in reversed(self.children):
            await child.cleanup(db, with_tat_new)
        await db.execute(f'drop trigger if exists store__tat_delta on {self.table_name};')
        await db.execute(f'drop function if exists "{self.table_name}__store_delta"();')
        await db.execute(f'drop function if exists "{self.table_name}__apply_delta"();')
        await db.execute(f'drop table if exists {self.table_name}__tat_delta;')
        if with_tat_new:
            await db.execute(f'drop table if exists {self.table_name}__tat_new;')

    def check_sub_table(self):
        if self.table['inherits'] and not self.is_sub_table:
            parent = self.table['inherits'][0]
            raise Exception(f'table {self.table_name} is partition of table {parent}, '
                            f'you need alter {parent} instead')
        if self.table['inherits'] and len(self.table['inherits']) > 1:
            raise Exception('Multi inherits not supported')

    async def run(self):
        ts = time.time()
        await self.db.init_pool()
        await self.get_table_info()

        if self.args.cleanup:
            await self.cancel_autovacuum()
            await self.cleanup()
            return

        self.log(f'start ({self.table["pretty_size"]})')
        try:
            self.check_sub_table()
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
        self.log(f'done in {self.duration(ts)}\n')
