create foreign table analytics.hit_2023_12 (
  id bigint not null default nextval('analytics.hit_id_seq'::regclass),
  session_id bigint not null,
  ts timestamp without time zone not null,
  duration integer
)
inherits (analytics.hit)
server tat_test_freeze
options (schema_name 'freeze_db', table_name 'hit_2023_12');
