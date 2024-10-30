create table analytics.page (
  id bigserial,
  url integer
);

alter table analytics.page add constraint pk_page
  primary key (id);
