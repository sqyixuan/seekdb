##mysql租户库插入数据
connection tentsql_conn;
use testsql;
create table t2(c1 int, c2 int);
create table t2_part(c1 int, c2 int) partition by hash(c1) partitions 8;
#通过c1的hash值将数据均匀分为8部分
insert into t2 values(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
insert into t2_part values(1, 1);
insert into t2_part values(2, 2);
insert into t2_part values(3, 3);
insert into t2_part values(4, 4);
insert into t2_part values(5, 5);
insert into t2_part values(6, 6);
insert into t2_part values(7, 7);
insert into t2_part values(8, 8);
insert into t2_part values(9, 9);
insert into t2_part values(10, 10);
update t2 set c2=22 where c1=2;
update t2_part set c2=22 where c1=2;
delete from t2 where c1=9;
delete from t2_part where c1=9;
insert into t_test_trg_mysql values (1);
SELECT my_func1('You');
create database testsql1;
use testsql1;
create table t3(c1 int, c2 int);
insert into t3 values(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
drop database testsql1;
source t/compat/include_for_obtest/pl_after_oracle_obdocker.inc;
#oracle租户插入数据
connection tentora_conn;
create table t_new(c1 int, c2 int, c3 int, primary key(c1));
insert into t_new values(1, 5, 1), (2, 4, 2), (3, 3, 3), (4, 2, 4), (5, 1, 5),(6,2,6),(7,5,7),(8,2,8),(9,3,9);
update t_new set c2=22 where c1=2;
delete from t_new where c1=9;
insert into t_test_trg_ora values (1);

#仲裁租户插入数据
connection tentsql_conn;
use testsvr;
--disable_query_log
--disable_result_log
create table t_new(c1 int,c2 varchar(65536),c3 varchar(65536),c4 varchar(65536));
let $cnt=1;
while($cnt <= 50)
{
    eval insert into t_new (c1,c2,c3,c4) values($cnt,"c2","c3","c4");
    inc $cnt;
}
--enable_query_log
--enable_result_log
update t_new set c2="cc" where c1=2;
delete from t_new where c1=9;
