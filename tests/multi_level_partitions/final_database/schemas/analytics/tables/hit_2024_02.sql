create table analytics.hit_2024_02 (
  id bigint not null default nextval('analytics.hit_id_seq'::regclass),
  session_id bigint not null,
  ts timestamp without time zone not null,
  duration integer
)
inherits (analytics.hit);

alter table analytics.hit_2024_02 add constraint pk_hit_2024_02
  primary key (id);

alter table analytics.hit_2024_02 add constraint chk_hit_ts
  check (ts >= '2024-02-01 00:00:00'::timestamp without time zone AND ts < '2024-03-01 00:00:00'::timestamp without time zone);

create index fki_hit_2024_02__session on analytics.hit_2024_02(session_id, ts);
