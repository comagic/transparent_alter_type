create table analytics.session_noloaded (
  id bigint not null default nextval('analytics.session_id_seq'::regclass),
  page_id bigint,
  ts timestamp without time zone not null,
  is_loaded boolean not null,
  duration integer
)
partition by range (ts);

alter table only analytics.session attach partition analytics.session_noloaded for values in (false, NULL);
