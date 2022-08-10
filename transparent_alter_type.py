#!/usr/bin/python3

import sys
import re
import argparse
import psycopg2
from select import select
import psycopg2.extras
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE
import threading
import queue
import time
import signal
import datetime
from pg_export.acl import acl_to_grants


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


class TAT:
    def __init__(self, args):
        self.host = args.host
        self.port = args.port
        self.dbname = args.dbname
        self.table_name = args.table_name
        self.jobs = args.jobs
        self.lock_timeout = '%ss' % args.lock_timeout
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
        if self.execute('''select pg_cancel_backend(pid)
                             from pg_stat_activity
                            where state = 'active' and
                                  backend_type = 'autovacuum worker' and
                                  query ~ '%(name)s'; ''' % self.table):
            print('autovacuum canceled')

    def cancel_all_autovacuum(self):
        if self.execute('''select pg_cancel_backend(pid)
                             from pg_stat_activity
                            where state = 'active' and
                                  backend_type = 'autovacuum worker';'''):
            print('autovacuum canceled')

    def get_table_info(self):
        self.start_transaction()

        res = self.execute('''
            select tn.table_name as name,
                   t.relname as name_without_schema,
                   pg_size_pretty(pg_total_relation_size(t.oid)) as pretty_size,
                   pg_size_pretty(pg_relation_size(t.oid)) as pretty_data_size,
                   att.all_columns,
                   att.column_types,
                   pk.pk_columns,
                   d.comment,
                   i.create_indexes,
                   i.rename_indexes,
                   chk.create_constraints as create_check_constraints,
                   fk.drop_constraints,
                   uni.create_constraints || fk.create_constraints as create_constraints,
                   fk.validate_constraints,
                   p.grant_privileges,
                   tg.create_triggers,
                   v.drop_views,
                   v.create_views,
                   v.view_acl_to_grants_params,
                   v.comment_views,
                   f.drop_functions,
                   f.create_functions,
                   f.function_acl_to_grants_params,
                   att.alter_sequences,
                   sp.storage_parameters
              from pg_class t
             cross join lateral (select t.oid::regclass::text as table_name) tn
              left join lateral (select format('comment on table %%s__tat_new is %%L;',
                                               tn.table_name,
                                               d.description) as comment
                                   from pg_description d
                                  where d.objoid = t.oid and
                                        d.objsubid = 0 and
                                        d.classoid = 'pg_class'::regclass) d
                     on true
             cross join lateral (select coalesce(array_agg(replace(replace(pg_get_indexdef(i.indexrelid),
                                                                           ' ON ',
                                                                           '__tat_new ON '),
                                                                   ' USING ',
                                                                   '__tat_new USING ')
                                                           order by cardinality(i.indkey) desc),
                                                 '{}') as create_indexes,
                                        coalesce(array_agg(format('alter index %%s.%%s rename to %%s;',
                                                                  icn.nspname,
                                                                  (ic.relname || '__tat_new')::name,
                                                                  ic.relname)),
                                                 '{}') as rename_indexes
                                   from pg_index i
                                  inner join pg_class ic
                                          on ic.oid = i.indexrelid
                                  inner join pg_namespace icn
                                          on icn.oid = ic.relnamespace
                                  where i.indrelid = t.oid and
                                        ic.relname not like '%%\_tat') i
              left join lateral (select uni.contype,
                                        array_agg(a.attname order by a.attnum) filter (where a.attnotnull) as pk_columns,
                                        count(1)
                                   from pg_constraint uni
                                  inner join pg_attribute a
                                          on a.attnum = any(uni.conkey)
                                  where uni.conrelid = t.oid and
                                        uni.contype in ('p', 'u') and
                                        a.attrelid = t.oid
                                  group by 1
                                 having cardinality(array_agg(a.attname order by a.attnum) filter (where a.attnotnull)) = count(1) --only all not null columns
                                  order by 3
                                  limit 1) as pk on true
             cross join lateral (select coalesce(array_agg(format(
                                                             'alter table %%s add constraint %%s %%s using index %%s %%s %%s;',
                                                             tn.table_name,
                                                             uni.conname,
                                                             case
                                                               when uni.contype = 'p'
                                                                 then 'primary key'
                                                               else 'unique'
                                                             end,
                                                             uni.conname,
                                                             case
                                                               when uni.condeferrable
                                                                 then 'deferrable'
                                                             end,
                                                             case
                                                               when uni.condeferred
                                                                 then 'initially deferred'
                                                             end)),
                                                 '{}') as create_constraints
                                   from pg_constraint uni
                                  where uni.conrelid = t.oid and
                                        uni.contype in ('p', 'u')) uni
             cross join lateral (select coalesce(array_agg(format('alter table %%s__tat_new add constraint %%s %%s;',
                                                                  tn.table_name,
                                                                  chk.conname,
                                                                  pg_get_constraintdef(chk.oid))),
                                                 '{}') as create_constraints
                                   from pg_constraint chk
                                  where chk.conrelid = t.oid and
                                        chk.contype = 'c' and
                                        chk.conname not like '%%\_tat') chk
             cross join lateral (select coalesce(array_agg(format('alter table %%s add constraint %%s %%s not valid;',
                                                                  fk.conrelid::regclass::text,
                                                                  fk.conname,
                                                                  pg_get_constraintdef(fk.oid))),
                                                 '{}') as create_constraints,
                                        coalesce(array_agg(format('alter table %%s validate constraint %%s;',
                                                                  fk.conrelid::regclass::text,
                                                                  fk.conname)),
                                                 '{}') as validate_constraints,
                                        coalesce(array_agg(format('alter table %%s drop constraint %%s;',
                                                                  fk.conrelid::regclass::text,
                                                                  fk.conname))
                                                          filter (where fk.conrelid <> t.oid),
                                                 '{}') as drop_constraints
                                   from pg_constraint fk
                                  where (fk.conrelid = t.oid
                                         or
                                         fk.confrelid = t.oid) and
                                        fk.contype = 'f') fk
             cross join lateral (select coalesce(array_agg(format('grant %%s on table %%s__tat_new to "%%s";',
                                                                  p.privileges,
                                                                  tn.table_name,
                                                                  p.grantee)),
                                                 '{}') as grant_privileges
                                   from (select g.grantee, string_agg(g.privilege_type, ', ') as privileges
                                           from information_schema.role_table_grants g
                                          where g.table_name = t.relname and
                                                g.table_schema = t.relnamespace::regnamespace::text and
                                                g.grantee <> 'postgres'
                                          group by g.grantee) p) p
             cross join lateral (select coalesce(array_agg(pg_get_triggerdef(tg.oid) || ';'), '{}') as create_triggers
                                   from pg_trigger tg
                                  where tg.tgrelid = t.oid and
                                        not tgisinternal) tg
             cross join lateral (select array_agg(a.attname) as all_columns,
                                        json_object_agg(a.attname, a.atttypid::regtype) as column_types,
                                        coalesce(array_agg(format('alter sequence %%s owned by %%s__tat_new.%%s;',
                                                                  s.serial_sequence,
                                                                  tn.table_name,
                                                                  a.attname))
                                                          filter (where s.serial_sequence is not null),
                                                 '{}') as alter_sequences
                                   from pg_attribute a
                                  cross join pg_get_serial_sequence(tn.table_name, a.attname) as s(serial_sequence)
                                  where a.attrelid = t.oid and
                                        a.attnum > 0 and
                                        not a.attisdropped) att
             cross join lateral (select coalesce(array_agg(format('create view %%s as %%s; %%s;',
                                                                  v.oid::regclass::text,
                                                                  replace(replace(replace(pg_get_viewdef(v.oid),
                                                                                          format('timezone(''Europe/Moscow''::text, %%s.start_time) AS start_time', t.relname),
                                                                                          format('%%s.start_time', t.relname)),
                                                                                  format('timezone(''Europe/Moscow''::text, %%s.date_time) AS date_time', t.relname),
                                                                                  format('%%s.date_time', t.relname)),
                                                                          format('timezone(''Europe/Moscow''::text, %%s.hit_time) AS hit_time', t.relname),
                                                                          format('%%s.hit_time', t.relname)),
                                                                  (select string_agg(format('%%s; %%s;', pg_get_functiondef(pgt.tgfoid), pg_get_triggerdef(pgt.oid)), E';\n')
                                                                     from pg_trigger pgt
                                                                    where pgt.tgrelid = v.oid::regclass))
                                                           order by v.oid),
                                                 '{}') as create_views,
                                        coalesce(json_agg(json_build_object('obj_name', v.oid::regclass,
                                                                            'obj_type', 'table',
                                                                            'acl', v.relacl))
                                                         filter (where v.relacl is not null),
                                                 '[]') as view_acl_to_grants_params,
                                        coalesce(array_agg(format('comment on view %%s is %%L;',
                                                                  v.oid::regclass, d.description))
                                                          filter (where d.description is not null),
                                                 '{}') as comment_views,
                                        coalesce(array_agg(format('drop view %%s;',
                                                                  v.oid::regclass)
                                                           order by v.oid desc),
                                                 '{}') as drop_views,
                                        array_agg(v.reltype) as view_type_oids
                                   from pg_class v
                                   left join pg_description d
                                          on d.objoid = v.oid
                                  where v.relkind = 'v' and
                                        v.oid in (with recursive w_depend as (
                                                    select rw.ev_class
                                                      from pg_depend d
                                                     inner join pg_rewrite rw
                                                             on rw.oid = d.objid
                                                     where d.refobjid = t.oid
                                                    union
                                                    select rw.ev_class
                                                      from w_depend w
                                                     inner join pg_depend d
                                                             on d.refobjid = w.ev_class
                                                     inner join pg_rewrite rw
                                                             on rw.oid = d.objid
                                                  )
                                                  select d.ev_class
                                                    from w_depend d)) v
             cross join lateral (select coalesce(array_agg(pg_catalog.pg_get_functiondef(f.oid) || ';'), '{}') as create_functions,
                                        coalesce(json_agg(json_build_object(
                                                            'obj_name', format('%%s(%%s)', f.oid::regproc, pg_get_function_identity_arguments(f.oid)),
                                                            'obj_type', case
                                                                          when f.prokind = 'p'
                                                                            then 'procedure'
                                                                          else 'function'
                                                                        end,
                                                            'acl', f.proacl))
                                                         filter (where f.proacl is not null),
                                                 '[]') as function_acl_to_grants_params,
                                        coalesce(array_agg(format('drop function %%s(%%s);',
                                                                  f.oid::regproc::text,
                                                                  pg_get_function_identity_arguments(f.oid))),
                                                 '{}') as drop_functions
                                   from pg_proc f
                                  where f.prorettype = t.reltype
                                        or
                                        t.reltype = any(f.proargtypes)
                                        or
                                        t.reltype = any(f.proallargtypes)
                                        or
                                        f.prorettype = any(v.view_type_oids)) f
             cross join lateral (select coalesce(array_agg(format('alter table %%s set (%%s);', t.oid::regclass, ro.option)), '{}') as storage_parameters
                                   from unnest(t.reloptions) as ro(option)) sp
             where t.oid = %s::regclass''', self.table_name)

        if not res:
            raise Exception('table not found')

        if not res[0]['pk_columns']:
            raise Exception('table %(name)s does not have primary key or not null unique constraint' % res[0])

        if not self.is_force:
            columns_to_alter = []
            for column in self.columns:
                pg_type = self.execute('select %s::regtype as mnemonic', column['type'])[0]['mnemonic']
                if res[0]['column_types'][column['column']] == pg_type:
                    print('column %s.%s has already type %s' % (self.table_name, column['column'], pg_type))
                else:
                    columns_to_alter.append(column)
            if len(columns_to_alter) == 0:
                print('no column to alter, use --force to alter anyway')
                sys.exit(0)
            columns = columns_to_alter

        self.table = res[0]

    def create_table_new(self):
        self.start_transaction()

        print('%(name)s (%(pretty_size)s):' % self.table)
        print('  create %(name)s__tat_new ...' % self.table, end='')
        sys.stdout.flush()

        self.execute('''
            create table %(name)s__tat_new(
              like %(name)s
              including all
              excluding indexes
              excluding constraints
              excluding statistics)''' % self.table)

        if self.columns:
            self.execute(''.join('''
                alter table %(name)s__tat_new
                  alter column %(column)s
                    type %(type)s using (%(column)s::%(type)s);''' %
                dict(self.table, **c)
                for c in self.columns))

        self.execute('\n'.join(self.table['create_check_constraints']))
        self.execute('\n'.join(self.table['grant_privileges']))
        self.execute(self.table['comment'])
        self.cancel_autovacuum()
        self.execute('''
            alter table %(name)s          set (autovacuum_enabled = false);
            alter table %(name)s__tat_new set (autovacuum_enabled = false);''' % self.table)

        self.commit()
        print('done')

    def create_table_delta(self):
        print('  create %(name)s__tat_delta ...' % self.table, end='')
        sys.stdout.flush()
        self.start_transaction()

        self.execute('''
            create unlogged table %(name)s__tat_delta(
              like %(name)s excluding all)''' % self.table)

        self.execute('''alter table %(name)s__tat_delta add column tat_delta_id serial;
                        alter table %(name)s__tat_delta add column tat_delta_op "char";''' % self.table)

        self.execute('''create or replace
                        function "%(name)s__tat_delta"() returns trigger as $$
                        begin
                          if tg_op = 'INSERT' then
                            insert into %(name)s__tat_delta
                              values (new.*, default, 'i');

                          elsif tg_op = 'UPDATE' then
                            insert into %(name)s__tat_delta
                              values (new.*, default, 'u');

                          elsif tg_op = 'DELETE' then
                            insert into %(name)s__tat_delta
                              values (old.*, default, 'd');

                            return old;
                          end if;

                          return new;
                        end;
                        $$ language plpgsql security definer;''' % self.table)

        columns = ', '.join('"%s"' % c
                                for c in self.table['all_columns'])
        val_columns = ', '.join('r."%s"' % c
                                for c in self.table['all_columns'])
        where = ' and '.join('t."%s" = r."%s"' % (c, c)
                             for c in self.table['pk_columns'])
        set_columns = ','.join('"%s" = r."%s"' % (c, c)
                               for c in self.table['all_columns']
                               if c not in self.table['pk_columns'])

        self.execute('''create or replace
                        function "%(name)s__apply_delta"() returns integer as $$
                        declare
                          r record;
                          rows integer := 0;
                        begin
                          for r in with d as (
                                     delete from %(name)s__tat_delta returning *
                                   )
                                   select *
                                     from d
                                    order by tat_delta_id
                          loop
                            if r.tat_delta_op = 'i' then
                              insert into %(name)s__tat_new(%(columns)s)
                                values (%(val_columns)s)
                                on conflict do nothing;

                            elsif r.tat_delta_op = 'u' then
                              update %(name)s__tat_new t
                                 set %(set_columns)s
                               where %(where)s;

                            elsif r.tat_delta_op = 'd' then
                              delete from %(name)s__tat_new t
                               where %(where)s;
                            end if;

                            rows := rows + 1;
                          end loop;

                          return rows;
                        end;
                        $$ language plpgsql security definer;''' % dict(self.table, **locals()))

        self.cancel_autovacuum()
        self.execute('''create trigger replicate__tat_delta
                          after insert or delete or update on %(name)s
                          for each row execute procedure "%(name)s__tat_delta"();''' % self.table)
        self.commit()
        print('done')

    def copy_data(self):
        start_time = time.time()
        print('  copy data (%(pretty_data_size)s) ...' % self.table, end='')
        sys.stdout.flush()
        self.start_transaction()
        self.execute('insert into %(name)s__tat_new select * from %(name)s' % self.table)
        self.commit()
        print('done in', self.duration(time.time() - start_time))
        sys.stdout.flush()

    def create_indexes(self):
        start_time = time.time()
        print('  create %s indexes on %s jobs:' % (len(self.table['create_indexes']), self.jobs))
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
                self.output_queue.put('    start %s' % index_name)
                cursor = connect.cursor()
                cursor.execute(index_def)
                connect.commit()
                cursor.close()
                self.output_queue.put('    done %s in %s' % (index_name, self.duration(time.time() - start_time)))
        except Exception as e:
            self.exception_on_create_index = True
            raise e

    def apply_delta(self):
        start_time = time.time()
        print('    apply_delta ...', end='')
        sys.stdout.flush()
        rows = self.execute('select "%(name)s__apply_delta"() as rows;' % self.table)[0]['rows']

        print(rows, 'rows done in', self.duration(time.time() - start_time))
        return rows

    def analyze(self):
        start_time = time.time()
        print('  analyze ...', end='')
        sys.stdout.flush()

        self.start_transaction()
        self.execute('analyze %(name)s__tat_new' % self.table)
        self.commit()
        print('done in', self.duration(time.time() - start_time))

    def exclusive_lock_table(self):
        self.start_transaction()
        self.cancel_autovacuum()
        print('    lock table %(name)s ...' % self.table, end='')
        sys.stdout.flush()
        try:
            self.execute('lock table %(name)s in access exclusive mode' % self.table)
        except (psycopg2.errors.LockNotAvailable, psycopg2.errors.DeadlockDetected) as e:
            self.rollback()
            print('failed:', e)
            return False
        print('done')
        return True

    def restore_storage_parameters(self):
        self.execute('alter table %(name)s reset (autovacuum_enabled);' % self.table)
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
            print('    drop table %(name)s' % self.table)
            self.execute('drop table %(name)s;' % self.table)
            self.execute('drop function "%(name)s__tat_delta"();' % self.table)
            self.execute('drop function "%(name)s__apply_delta"();' % self.table)
            self.execute('drop table %(name)s__tat_delta;' % self.table)
            print('    rename table %(name)s__tat_new -> %(name)s' % self.table)
            self.execute('alter table %(name)s__tat_new rename to %(name_without_schema)s;' % self.table)
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
        print('  validate %s constraints:' % len(self.table['validate_constraints']))
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
            print('    pause failed: %s: %s' % (e.__class__.__name__, e))
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
            print('resume failed: %s: %s' % (e.__class__.__name__, e))

    def cleanup(self):
        self.start_transaction()
        self.execute('drop trigger if exists replicate__tat_delta on %(name)s;' % self.table)
        self.execute('drop function if exists "%(name)s__tat_delta"();' % self.table)
        self.execute('drop function if exists "%(name)s__apply_delta"();' % self.table)
        self.execute('drop table if exists %(name)s__tat_delta;' % self.table)
        self.execute('drop table if exists %(name)s__tat_new;' % self.table)
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
psycopg2.extensions.set_wait_callback(wait_select_inter)

t = TAT(args)
t.run()
