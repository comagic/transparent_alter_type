#!/bin/bash

export PGPASSWORD=123456
export PGUSER=postgres
export PGHOST=0.0.0.0
export PGPORT=5432
export PGDATABASE=tat_test
PG_VERSION=15

docker rm -f pg_tat_test

set -e

if [ $# -eq 1 ]; then
    PG_VERSION=$1
fi

docker run --name pg_tat_test --tmpfs=/var/lib/postgresql -p $PGPORT:5432 -e POSTGRES_PASSWORD=$PGPASSWORD -d postgres:$PG_VERSION
sleep 1.5
echo "build src database"
psql -c "create database $PGDATABASE" -d postgres
pg_import source_database -d $PGDATABASE
psql -f source_database/publications/logical_replica.sql

echo "======================================================="
psql -c "insert into analytics.communication(id, type, duration)
         select i, type, i % 100
           from generate_series(1, 1000000) i
          cross join unnest(enum_range(null::analytics.communication_type_mnemonic)) as type"

# batch mode multi column primary key
transparent_alter_type -t analytics.communication -c "id:bigint" --copy-data-jobs 2 --create-index-jobs 4 --batch-size 100000 &

sleep 1.1  # the following 3 commands will be executed in parallel with transparent_alter_type
psql -c "update analytics.communication
            set duration = duration + 20
          where id between 20 and 30"
psql -c "delete from analytics.communication
          where id > 200"
psql -c "insert into analytics.communication(id, type, duration)
         select i, type, i % 100
           from generate_series(1000001, 1000100) i
          cross join unnest(enum_range(null::analytics.communication_type_mnemonic)) as type"
wait

echo "======================================================="
psql -c "insert into analytics.page(url)
         select generate_series(1, 1000000)"

# batch mode single column primary key
transparent_alter_type -t analytics.page -c "id:bigint" --copy-data-jobs 2 --create-index-jobs 4 --batch-size 100000 &

sleep 1.1  # the following 3 commands will be executed in parallel with transparent_alter_type
psql -c "update analytics.page
            set url = url + 20
          where id between 20 and 30"
psql -c "delete from analytics.page
          where id > 200"
psql -c "insert into analytics.page(url)
         select generate_series(1, 100)"
wait

echo "======================================================="
psql -c "insert into analytics.session(page_id, ts, is_loaded, duration)
         select i % 200 + 1, '2024-01-01'::date + (random() * 58)::int, random() < 0.1, i % 10
           from generate_series(1, 1000000) i"

# declarative partitioning
transparent_alter_type -t analytics.session -c "id:bigint" -c "page_id:bigint" --copy-data-jobs 2 --create-index-jobs 4 &

sleep 1  # the following 3 commands will be executed in parallel with transparent_alter_type
psql -c "update analytics.session
            set duration = duration + 20
          where id < 1000"
psql -c "delete from analytics.session
          where id > 2000"
psql -c "insert into analytics.session(page_id, ts, is_loaded, duration)
         select i % 200 + 1, '2024-01-01'::date + (random() * 58)::int, random() < 0.1, i % 10
           from generate_series(1, 1000) i"
psql -c "insert into analytics.session(page_id, ts, is_loaded, duration)
         select i % 200 + 1, '2023-12-01'::date + (random() * 20)::int, false, i % 10
           from generate_series(1, 100) i"
wait

echo "======================================================="
psql -c "insert into analytics.hit_2024_01(session_id, ts, duration)
         select i % 200 + 1, '2024-01-01'::date + (random() * 20)::int,  i % 10
           from generate_series(1, 1000000) i"

# old style inheritance partitioning
transparent_alter_type -t analytics.hit -c "id:bigint" -c "session_id:bigint" --copy-data-jobs 2 --create-index-jobs 4 &

sleep 1  # the following 3 commands will be executed in parallel with transparent_alter_type
psql -c "update analytics.hit
            set duration = duration + 20
          where id < 1000"
psql -c "delete from analytics.hit
          where id > 2000"
psql -c "insert into analytics.hit_2024_02(session_id, ts, duration)
         select i % 200 + 1, '2024-02-01'::date + (random() * 20)::int,  i % 10
           from generate_series(1, 1000) i"
psql -c "insert into analytics.hit_2023_12(session_id, ts, duration)
         select i % 200 + 1, '2023-12-01'::date + (random() * 20)::int,  i % 10
           from generate_series(1, 100) i"
wait
echo
echo "======================================================="
echo "diff table structure:"
pg_export $PGDATABASE /tmp/exp_tat_test
diff -x "public.sql" -qr /tmp/exp_tat_test/schemas/ final_database/schemas && echo " all tables: ok"
diff -qr /tmp/exp_tat_test/publications final_database/publications && echo " publications: ok"
echo
echo "check sum:"
psql -t -c "select 'analytics.communication: ' ||
                   case
                     when count(1) = 900 and sum(duration) = 45210
                       then 'ok'
                     else 'FAILED'
                   end
              from analytics.communication" | grep -v "^$"

psql -t -c "select 'analytics.page: ' ||
                   case
                     when count(1) = 300 and sum(url) = 25370
                       then 'ok'
                     else 'FAILED'
                   end
              from analytics.page" | grep -v "^$"

psql -t -c "select 'analytics.session: ' ||
                   case
                     when count(1) = 3100 and sum(duration) = 33930
                       then 'ok'
                     else 'FAILED'
                   end
              from analytics.session" | grep -v "^$"

psql -t -c "select 'analytics.hit: ' ||
                   case
                     when count(1) = 3100 and sum(duration) = 33930
                       then 'ok'
                     else 'FAILED'
                   end
              from analytics.hit"

#psql -t -c "select count(1), sum(url)
#              from analytics.page"
#psql -t -c "select count(1), sum(duration)
#              from analytics.session"
#psql -t -c "select count(1), sum(duration)
#              from analytics.hit"
#psql -t -c "select count(1), sum(duration)
#              from analytics.communication"
