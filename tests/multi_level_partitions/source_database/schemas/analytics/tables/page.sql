create table analytics.page (
  id serial,
  url integer
);

alter table analytics.page add constraint pk_page
  primary key (id);

alter table analytics.page replica identity full;