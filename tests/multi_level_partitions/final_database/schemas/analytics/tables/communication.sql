create table analytics.communication (
  id bigint not null,
  type analytics.communication_type_mnemonic not null,
  duration integer
);

alter table analytics.communication add constraint pk_communication
  primary key (id, type);
