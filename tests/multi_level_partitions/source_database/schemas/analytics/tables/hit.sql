create table analytics.hit (
  id serial,
  session_id integer not null,
  ts timestamp without time zone not null,
  duration integer
);

alter table analytics.hit add constraint pk_hit
  primary key (id);

create index fki_hit__session on analytics.hit(session_id, ts);
