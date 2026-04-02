
--disable_warnings
drop table if exists t1,t2,t3;
--enable_warnings
eval create table t1 (a tinyint unsigned not null, primary key(a))  
partition by key (a) ;
#partition by key (a) (
#partition pa1 max_rows=20 min_rows=2,
#partition pa2 max_rows=30 min_rows=3,
#partition pa3 max_rows=30 min_rows=4,
#partition pa4 max_rows=40 min_rows=2);
--source mysql_test/include/show_create_table_old_version.inc
show create table t1;
insert into t1 values (255);
insert into t1 values  (254);
insert into t1 values  (253);
insert into t1 values  (252);
insert into t1 values  (1);
insert into t1 values  (2);
insert into t1 values  (128);
--sorted_result
select * from t1;
select * from t1 where a=253;
delete from t1 where a=253;
--sorted_result
select * from t1;
drop table t1;

eval create table t2 (a tinyint unsigned not null, primary key(a))  
partition by key (a) partitions 8;
--source mysql_test/include/show_create_table_old_version.inc
show create table t2;
insert into t2 values (255);
insert into t2 values  (254);
insert into t2 values  (253);
insert into t2 values  (252);
--sorted_result
select * from t2;
select * from t2 where a=253;
delete from t2 where a=253;
--sorted_result
select * from t2;
delete from t2;
let $count=255;
--echo 255 inserts;
--disable_query_log
while ($count)
{
eval replace into t2 values ($count);
dec $count;
}
--enable_query_log
select count(*) from t2;
drop table t2;

eval create table t3 (a tinyint not null, primary key(a))  
partition by key (a) partitions 7;
--source mysql_test/include/show_create_table_old_version.inc
show create table t3;
insert into t3 values (127);
insert into t3 values  (126);
insert into t3 values  (125);
insert into t3 values  (124);
insert into t3 values  (-128);
insert into t3 values  (-127);
insert into t3 values  (1);
insert into t3 values  (-1);
insert into t3 values  (0);
--sorted_result
select * from t3;
select * from t3 where a=125;
delete from t3 where a=125;
--sorted_result
select * from t3;
drop table t3;
