eval alter session set current_schema = $dataDB;

create table base_table_to_flashback(
     col1 int,
     col2 int
);
create index base_index_to_flashback on base_table_to_flashback(col2);
drop table base_table_to_flashback;

create table base_table_to_truncate(
     col1 int,
     col2 VARCHAR(30));
create index base_index_to_truncate on base_table_to_truncate(col2);
truncate table base_table_to_truncate;
rename base_table_to_truncate to base_table_has_been_truncated;

create view base_view_to_flashback AS SELECT * FROM base_table_has_been_truncated;
drop view base_view_to_flashback;

# drop user 不进回收站
create user base_db_to_flashback identified by "";
alter session set current_schema = base_db_to_flashback;
create table base_table_in_recycle_db(
     col1 int,
     col2 VARCHAR(30)
);
create index base_index_in_recyle_db on base_table_in_recycle_db(col2);
create view base_view_in_recyle_db AS SELECT * FROM base_table_in_recycle_db;
drop user base_db_to_flashback cascade;

# drop user 不进回收站
create user base_db_to_flashback_and_rename identified by "";
drop user base_db_to_flashback_and_rename cascade;

eval alter session set current_schema = $dataDB;
