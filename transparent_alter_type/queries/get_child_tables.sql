with recursive wchld as (
  select i.inhrelid, 0 as level
    from pg_inherits i
   where i.inhparent = $1::regclass
  union all
  select i.inhrelid, wchld.level + 1 as level
    from wchld
   inner join pg_inherits i
           on i.inhparent = wchld.inhrelid
)
select coalesce(
         array_agg(wchld.inhrelid::regclass::text
                   order by wchld.level, wchld.inhrelid::regclass::text),
         '{}') as children
  from wchld
