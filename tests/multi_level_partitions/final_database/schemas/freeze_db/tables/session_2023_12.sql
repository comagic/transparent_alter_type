create table freeze_db.session_2023_12 (
  id integer not null default nextval('analytics.session_id_seq'::regclass),
  page_id integer,
  ts timestamp without time zone not null,
  is_loaded boolean not null,
  duration integer
);

alter table freeze_db.session_2023_12 add constraint pk_session_2023_12
  primary key (id);
