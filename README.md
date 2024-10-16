transparent_alter_type
======================

transparent_alter_type - tools for alter type of columns without locks.

# Installation

$ pip install transparent-alter-type 

# Dependency

* python3.8+

# Usage

    usage: transparent_alter_type [--help] -t TABLE_NAME [-c COLUMN] [-h HOST] [-d DBNAME]
                              [-U USER] [-W PASSWORD] [-p PORT] [-j JOBS] [--force]
                              [--cleanup] [--lock-timeout LOCK_TIMEOUT]
                              [--time-between-locks TIME_BETWEEN_LOCKS]
                              [--work-mem WORK_MEM] [--min-delta-rows MIN_DELTA_ROWS]
                              [--skip-fk-validation] [--show-queries]
                              

# How it works

1. create new tables TABLE_NAME__tat_new (with new column type) and TABLE_NAME__tat_delta
2. create trigger replicate__tat_delta wich fixing all changes on TABLE_NAME to TABLE_NAME__tat_delta
3. copy data from TABLE_NAME to TABLE_NAME__tat_new
4. create indexes for TABLE_NAME__tat_new (in parallel mode on JOBS)
5. analyze TABLE_NAME__tat_new
6. apply delta from TABLE_NAME__tat_delta to TABLE_NAME__tat_new (in loop while last rows > MIN_DELTA_ROWS)
7. begin;
   drop depend functions, views, constraints;
   link sequences to TABLE_NAME__tat_new
   drop table TABLE_NAME;
   apply delta;
   rename table TABLE_NAME__tat_new to TABLE_NAME;
   create depend functions, views, constraints (not valid);
   commit;
8. validate constraints

# Quick examples

    ./transparent_alter_type.py -h 127.0.0.1 -p 5432 -d billing -j 8 -t account -c "balance:numeric(14,4)" -c "dept_limit:numeric(14,4)"
