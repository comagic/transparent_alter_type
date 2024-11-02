create publication logical_replica;

alter publication logical_replica add table analytics.hit_2024_01;
alter publication logical_replica add table analytics.hit_2024_02;
alter publication logical_replica add table analytics.page;
