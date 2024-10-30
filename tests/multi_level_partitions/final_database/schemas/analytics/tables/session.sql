create table analytics.session (
  id bigserial,
  page_id bigint,
  ts timestamp without time zone not null,
  is_loaded boolean not null,
  duration integer
)
partition by list (is_loaded);
