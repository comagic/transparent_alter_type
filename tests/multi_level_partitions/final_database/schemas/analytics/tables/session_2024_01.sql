create table analytics.session_2024_01 (
  id bigint not null default nextval('analytics.session_id_seq'::regclass),
  page_id bigint,
  ts timestamp without time zone not null,
  is_loaded boolean not null,
  duration integer
);

alter table only analytics.session_noloaded attach partition analytics.session_2024_01 for values from ('2024-01-01 00:00:00') to ('2024-02-01 00:00:00');

alter table analytics.session_2024_01 add constraint pk_session_2024_01
  primary key (id);

alter table analytics.session_2024_01 add constraint fk_session__page
  foreign key (page_id) references analytics.page(id);

create index idx_session_2024_01 on analytics.session_2024_01(ts);

alter table analytics.session_2024_01 replica identity using index pk_session_2024_01;
