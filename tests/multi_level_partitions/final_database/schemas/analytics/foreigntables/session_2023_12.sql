create foreign table analytics.session_2023_12 (
  id bigint not null default nextval('analytics.session_id_seq'::regclass),
  page_id bigint,
  ts timestamp without time zone not null,
  is_loaded boolean not null,
  duration integer
)
server tat_test_freeze
options (schema_name 'freeze_db', table_name 'session_2023_12');

alter table only analytics.session_noloaded attach partition analytics.session_2023_12 for values from ('2023-12-01 00:00:00') to ('2024-01-01 00:00:00');
