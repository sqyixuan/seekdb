connection tentsql_conn;
create database heap_table1;
use heap_table1;
create table test (c1 int, c2 int, c3 varchar(10), c4 int, c5 int, primary key(c1, c2, c3)) ORGANIZATION HEAP;
alter system set _enable_parallel_table_creation = false;
create table test_not_parallel (c1 int, c2 int, c3 varchar(10), c4 int, c5 int, primary key(c1, c2, c3)) ORGANIZATION HEAP;
alter system set _enable_parallel_table_creation = true;
--error 1062
create table test(c1 int primary key, c2 int, primary key (c1,c2)) ORGANIZATION HEAP;
SET _show_ddl_in_compat_mode = 1;
show create table test;
SET _show_ddl_in_compat_mode = 0;
show create table test;

create table test_idx (c1 int, c2 int, c3 varchar(10), c4 int, c5 int, primary key(c1, c2, c3));
show create table test_idx;
--disable_all_result_log
let $heap_organization = query_get_value(select ORGANIZATION from information_schema.TABLES where TABLE_NAME = 'test' and TABLE_SCHEMA = 'heap_table1', ORGANIZATION, 1);
let $index_organization = query_get_value(select ORGANIZATION from information_schema.TABLES where TABLE_NAME = 'test_idx' and TABLE_SCHEMA = 'heap_table1', ORGANIZATION, 1);
if ($heap_organization != $index_organization)
{
  echo "success! two tables have different organization";
}
if ($heap_organization == $index_organization)
{
  --exit
}

connection proxy_conn;
let $heap_organization = query_get_value(select IOT_TYPE from CDB_TABLES where TABLE_NAME = 'test' and OWNER='heap_table1', IOT_TYPE, 1);
let $index_organization = query_get_value(select IOT_TYPE from CDB_TABLES where TABLE_NAME = 'test_idx' and OWNER='heap_table1', IOT_TYPE, 1);

if ($heap_organization != $index_organization)
{
  echo "success!";
}

if ($heap_organization == $index_organization)
{
  --exit
}

connection tentsql_conn;
use heap_table1;
let $cnt=1;
while($cnt <= 500)
{
  eval insert into  test(c1,c2,c3,c4,c5) values (1,1,$cnt,3,4);
  inc $cnt;
}
--enable_all_result_log
ALTER TABLE test ADD c6 varchar(80);
rename table test to test_heap;
ALTER TABLE test_heap MODIFY COLUMN c4 varchar(80);
