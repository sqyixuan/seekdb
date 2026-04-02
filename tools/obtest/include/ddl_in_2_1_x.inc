# case 1: MySQL
connection conn_obtest;
# sys_variable
ob1.mysql alter tenant obtest set variables ob_query_timeout = 20000000;
ob1.mysql alter tenant obtest set variables ob_query_timeout = 10000000;
set @@global.ob_query_timeout = 20000000;
set @@global.ob_query_timeout = 10000000;

# tablegroup(with partition)
create tablegroup schema_tg;
create tablegroup schema_tg_hash partition by hash partitions 2;
create tablegroup schema_tg_range partition by range (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create tablegroup schema_tg_range_hash partition by range subpartition by hash subpartitions 2 (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create tablegroup schema_tg_hash_range partition by hash subpartition by range subpartition template (SUBPARTITION p0 VALUES LESS THAN (1990),  SUBPARTITION p1 VALUES LESS THAN (2000),  SUBPARTITION p2 VALUES LESS THAN (MAXVALUE)) partitions 2;

# database
create database schema_db_with_tg tablegroup = schema_tg;
create database schema_db;

use schema_db;
# table with tablegroup、partition
create table schema_tb (c1 int primary key, c2 int) tablegroup = "schema_tg" comment = 'simple table';
create table schema_tb_hash (c1 int primary key, c2 int) tablegroup = "schema_tg_hash" partition by hash(c1) partitions 2;
create table schema_tb_range (c1 int primary key, c2 int) tablegroup = "schema_tg_range" partition by range(c1) (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create table schema_tb_range_hash (c1 int, c2 int, primary key(c1, c2)) tablegroup = "schema_tg_range_hash" partition by range(c1) subpartition by hash(c2) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create table schema_tb_hash_range (c1 int, c2 int, primary key(c1, c2)) tablegroup = "schema_tg_hash_range" partition by hash(c1) subpartition by range(c2) subpartition template (SUBPARTITION p0 VALUES LESS THAN (1990),  SUBPARTITION p1 VALUES LESS THAN (2000),  SUBPARTITION p2 VALUES LESS THAN (MAXVALUE)) partitions 2;

# table with constraint、sequence、index
create table schema_tb_complex (a int primary key auto_increment, b int, constraint c1 check (a = b), unique key uk_a_b (a, b));
create table schema_tb_foreign(pk int, a int, b int, primary key (pk), unique key uk_a_b (a, b), constraint foreign key (a, b) references schema_tb_complex (a, b) on update CASCADE on delete CASCADE);

# 临时表
create temporary table schema_tb_temp(c1 int, c2 varchar(10), c3 date);

set recyclebin = on;
# table in recyclebin
create table schema_tb_in_recyclebin(c1 int, c2 int);
drop table schema_tb_in_recyclebin;

# database in recyclebin
create database schema_db_in_recyclebin;
use schema_db_in_recyclebin;
create table schema_tb_in_recyclebin_1(c1 int, c2 int);
drop database schema_db_in_recyclebin;
set recyclebin = off;

# user
create user schema_user;

# privilege(table、database)
grant select on schema_db_with_tg.* to schema_user;
grant select on schema_db.schema_tb to schema_user;

use schema_db;
# outline
create outline schema_outline on select * from schema_tb;
# synonym
create synonym synonym1 for schema_db.schema_tb;

# routine
create table schema_tb_for_routine(a1 int,a2 int,a3 int);
CREATE PROCEDURE schema_routine(x INT, y INT) BEGIN set x=y; INSERT INTO schema_tb_for_routine VALUES(1, 1, 1); END;

# case 2: Oracle
connection conn_oracle;
# sys_variable
ob1.mysql alter tenant oracle set variables ob_query_timeout = 20000000;
ob1.mysql alter tenant oracle set variables ob_query_timeout = 10000000;
set @@global.ob_query_timeout = 20000000;
set @@global.ob_query_timeout = 10000000;

# tablegroup(with partition)
create tablegroup schema_tg;
create tablegroup schema_tg_hash partition by hash partitions 2;
create tablegroup schema_tg_range partition by range columns 1 (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create tablegroup schema_tg_range_hash partition by range columns 1 subpartition by hash subpartitions 2 (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create tablegroup schema_tg_hash_range partition by hash subpartition by range columns 1 subpartition template (SUBPARTITION p0 VALUES LESS THAN (1990),  SUBPARTITION p1 VALUES LESS THAN (2000),  SUBPARTITION p2 VALUES LESS THAN (MAXVALUE)) partitions 2;

# user(database)
create user schema_user identified by schema_user;
create user schema_other_user identified by schema_user;

# privilege
alter session set current_schema = SCHEMA_USER;
create table schema_user_table (c1 int primary key, c2 int);
alter session set current_schema = SYS;
grant all on schema_other_user.* to schema_user WITH GRANT OPTION;
grant all on sys.schema_user_table to schema_user WITH GRANT OPTION;
grant all on *.* to schema_user WITH GRANT OPTION;

alter session set current_schema = SCHEMA_USER;
# table with tablegroup、partition
create table schema_tb (c1 int primary key, c2 int) tablegroup = "SCHEMA_TG";
create table schema_tb_hash (c1 int primary key, c2 int) tablegroup = "SCHEMA_TG_HASH" partition by hash(c1) partitions 2;
create table schema_tb_range (c1 int primary key, c2 int) tablegroup = "SCHEMA_TG_RANGE" partition by range(c1) (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create table schema_tb_range_hash (c1 int, c2 int, primary key(c1, c2)) tablegroup = "SCHEMA_TG_RANGE_HASH" partition by range(c1) subpartition by hash(c2) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1990), PARTITION c1 VALUES LESS THAN (2000), PARTITION c2 VALUES LESS THAN MAXVALUE);
create table schema_tb_hash_range (c1 int, c2 int, primary key(c1, c2)) tablegroup = "SCHEMA_TG_HASH_RANGE" partition by hash(c1) subpartition by range(c2) subpartition template (SUBPARTITION p0 VALUES LESS THAN (1990),  SUBPARTITION p1 VALUES LESS THAN (2000),  SUBPARTITION p2 VALUES LESS THAN (MAXVALUE)) partitions 2;

# sequence
create sequence schema_sequence maxvalue 4611686018427387903 increment by 1;
# table with constraint (2.1 oracle only support primary key constraint)
create table schema_tb_complex (a int primary key, b int);
# index
create index schema_idx on schema_tb_complex(a, b);
# 2.1 oracle not support foreign key

# 临时表
create global temporary table schema_tb_temp(c1 int, c2 varchar(10));

set recyclebin = on;
# table in recyclebin
create table schema_tb_in_recyclebin(c1 int, c2 int);
drop table schema_tb_in_recyclebin;

# database in recyclebin
create user schema_db_in_recyclebin identified by schema_db_in_recyclebin;
alter session set current_schema = SCHEMA_DB_IN_RECYCLEBIN;
create table schema_tb_in_recyclebin_1(c1 int, c2 int);
drop user schema_db_in_recyclebin cascade;
set recyclebin = off;

alter session set current_schema = SCHEMA_USER;

# outline
create outline schema_outline on select * from schema_tb;

# synonym
create synonym synonym1 for schema_user.schema_tb;

# package
#delimiter /;
#create or replace package obpackagetest as
#  type ob_type_record is record(id int, msg varchar2(30));
#  type ob_type_nested_table is table of varchar2(30);
#  ob_item_var varchar2(30);
#  ob_item_rec ob_type_record;
#  ob_item_nested_table ob_type_nested_table;
#  function get_var return varchar2;
#  procedure set_var;
#end/;
#delimiter ;
#drop package obpackagetest;

# routine
create table schema_tb_for_routine(a1 int,a2 int,a3 int);
CREATE PROCEDURE schema_routine(x INT, y INT) BEGIN set x=y; INSERT INTO schema_tb_for_routine VALUES(1, 1, 1); END;
