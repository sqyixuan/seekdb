--disable_warnings
drop table if exists t1,t2;
--enable_warnings
eval create table t1 (a float not null, primary key(a))  partition by key (a) partitions 4;
#partition by key (a) (
#partition pa1 max_rows=20 min_rows=2,
#partition pa2 max_rows=30 min_rows=3,
#partition pa3 max_rows=30 min_rows=4,
#partition pa4 max_rows=40 min_rows=2);
--source mysql_test/include/show_create_table_old_version.inc
show create table t1;
insert into t1 values (-3.402823466E+38);
insert into t1 values (3.402823466E+38);
insert into t1 values (-1.5);
insert into t1 values (-1);
insert into t1 values (0);
insert into t1 values (1);
insert into t1 values (1.5);
select * from t1;
select * from t1 where a=1.5;
delete from t1 where a=1.5;
select * from t1;
drop table t1;

eval create table t2 (a float not null, primary key(a))  partition by key (a) partitions 10;
--source mysql_test/include/show_create_table_old_version.inc
show create table t2;
insert into t2 values (-3.402823466E+38);
insert into t2 values (-3.402823466E+37);
insert into t2 values (-123.456);
insert into t2 values (0);
insert into t2 values (1234546.789);
insert into t2 values (123.456);
insert into t2 values (1.5);
select * from t2;
#result set is empty: Not a bug with float!!
select * from t2 where a=123.456;
delete from t2 where a=123.456;
select * from t2;
select * from t2 where a=1.5;
delete from t2 where a=1.5;
select * from t2;
delete from t2;
let $count=$maxrows;
--echo $maxrows*3 inserts;
--disable_query_log
while ($count)
{
eval insert into t2 values ($count);
eval insert into t2 values ($count+0.33);
eval insert into t2 values ($count+0.75);
dec $count;
}
--enable_query_log
select count(*) from t2;
drop table t2;
