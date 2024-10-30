create table freeze_db.hit_2023_12 (
  id integer not null default nextval('analytics.hit_id_seq'::regclass),
  session_id integer not null,
  ts timestamp without time zone not null,
  duration integer
);

alter table freeze_db.hit_2023_12 add constraint pk_hit_2023_12
  primary key (id);
