create table analytics.session_loaded (
  id integer not null default nextval('analytics.session_id_seq'::regclass),
  page_id integer,
  ts timestamp without time zone not null,
  is_loaded boolean not null,
  duration integer
);

alter table only analytics.session attach partition analytics.session_loaded for values in (true);

alter table analytics.session_loaded add constraint pk_session_loaded
  primary key (id);

alter table analytics.session_loaded add constraint fk_session__page
  foreign key (page_id) references analytics.page(id);

create index idx_session_loaded on analytics.session_loaded(ts);

alter table analytics.session_loaded replica identity nothing;
