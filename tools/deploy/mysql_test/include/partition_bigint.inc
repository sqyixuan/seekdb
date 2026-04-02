
--disable_warnings
drop table if exists t1,t2,t3;
--enable_warnings
eval create table t1 (a bigint unsigned not null, primary key(a))
partition by key(a) ;
#partition by key (a) (
#partition pa1 max_rows=20 min_rows=2,
#partition pa2 max_rows=30 min_rows=3,
#partition pa3 max_rows=30 min_rows=4,
#partition pa4 max_rows=40 min_rows=2);
--source mysql_test/include/show_create_table_old_version.inc
show create table t1;
insert into t1 values (18446744073709551615);
insert into t1 values  (0xFFFFFFFFFFFFFFFE);
insert into t1 values  (18446744073709551613);
insert into t1 values  (18446744073709551612);
insert into t1 values  (1);
insert into t1 values  (2);
insert into t1 values  (65535);
--sorted_result
select * from t1;
select * from t1 where a=-2;
delete from t1 where a=-2;
--sorted_result
select * from t1;
select * from t1 where a=18446744073709551615;
delete from t1 where a=18446744073709551615;
--sorted_result
select * from t1;
drop table t1;

eval create table t2 (a bigint unsigned not null, primary key(a))  
partition by key (a) partitions 8;
--source mysql_test/include/show_create_table_old_version.inc
show create table t2;
insert into t2 values (18446744073709551615);
insert into t2 values  (0xFFFFFFFFFFFFFFFE);
insert into t2 values  (18446744073709551613);
insert into t2 values  (18446744073709551612);
--sorted_result
select * from t2;
select * from t2 where a=18446744073709551615;
delete from t2 where a=18446744073709551615;
--sorted_result
select * from t2;
delete from t2;
let $count=$maxrows;
--echo $maxrows inserts;
--disable_query_log
while ($count)
{
eval replace into t2 values ($count);
dec $count;
}
--enable_query_log
select count(*) from t2;
drop table t2;

eval create table t3 (a bigint not null, primary key(a))  
partition by key (a) partitions 7;
--source mysql_test/include/show_create_table_old_version.inc
show create table t3;
insert into t3 values (9223372036854775807);
insert into t3 values  (9223372036854775806);
insert into t3 values  (9223372036854775805);
insert into t3 values  (9223372036854775804);
insert into t3 values  (-9223372036854775808);
insert into t3 values  (-9223372036854775807);
insert into t3 values  (1);
insert into t3 values  (-1);
insert into t3 values  (0);
--sorted_result
select * from t3;
select * from t3 where a=9223372036854775806;
delete from t3 where a=9223372036854775806;
--sorted_result
select * from t3;
drop table t3;
