create table analytics.session_2024_02 (
  id integer not null default nextval('analytics.session_id_seq'::regclass),
  page_id integer,
  ts timestamp without time zone not null,
  is_loaded boolean not null,
  duration integer
);

alter table only analytics.session_noloaded attach partition analytics.session_2024_02 for values from ('2024-02-01 00:00:00') to ('2024-03-01 00:00:00');

alter table analytics.session_2024_02 add constraint pk_session_2024_02
  primary key (id);

alter table analytics.session_2024_02 add constraint fk_session__page
  foreign key (page_id) references analytics.page(id);

create index idx_session_2024_02 on analytics.session_2024_02(ts);

alter table analytics.session_2024_02 replica identity using index pk_session_2024_02;
