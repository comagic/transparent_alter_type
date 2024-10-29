create server tat_test_freeze
    foreign data wrapper postgres_fdw
    options (dbname 'tat_test',
             host '0.0.0.0',
             port '5432');

create user mapping
  for public
  server tat_test_freeze
  options ("user" 'postgres', "password" '123456');
