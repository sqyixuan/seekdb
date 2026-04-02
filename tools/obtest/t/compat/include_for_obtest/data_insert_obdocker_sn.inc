#mysql租户+仲裁租户插入数据
connection tentsql_conn;
create database testsql;
use testsql;
create table t1(c1 int, c2 int);
create table t_part(c1 int, c2 int) partition by hash(c1) partitions 8;
#通过c1的hash值将数据均匀分为8部分
insert into t1 values(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10);
insert into t_part values(1, 1);
insert into t_part values(2, 2);
insert into t_part values(3, 3);
insert into t_part values(4, 4);
insert into t_part values(5, 5);
insert into t_part values(6, 6);
insert into t_part values(7, 7);
insert into t_part values(8, 8);
insert into t_part values(9, 9);
insert into t_part values(10, 10);
update t1 set c2=22 where c1=2;
update t_part set c2=22 where c1=2;
delete from t1 where c1=9;
delete from t_part where c1=9;
create table t_test_trg_mysql(c1 int);
create trigger trg_t_test_trg_mysql before insert on t_test_trg_mysql for each row set @var_test_trg = 1;
source t/compat/include_for_obtest/pl_for_obdocker.inc;
connection tentsql_conn;
use testsql;
#全量旁路导入
CREATE TABLE tbl2_l (col1 BIGINT PRIMARY KEY, col2 VARCHAR(50)) PARTITION BY LIST (col1) (PARTITION p0 VALUES IN (1, 2, 3), PARTITION p1 VALUES IN (5, 6), PARTITION p2 VALUES IN (DEFAULT));
insert /*+ direct enable_parallel_dml parallel(N) */ into tbl2_l select * from tbl1_l;
create table t1_1(c1 int, c2 int);
insert into t1_1 values(11, 1), (12, 2), (13, 3), (14, 4), (15, 5), (16, 6), (17, 7), (18, 8), (19, 9), (20, 10);
create table t1_2(c1 int, c2 int);
insert into t1_2 values(21, 1), (22, 2), (23, 3), (24, 4), (25, 5), (26, 6), (27, 7), (28, 8), (29, 9), (30, 10);
connection tentsql_conn;
create database testsvr;
use testsvr;
--disable_query_log
--disable_result_log
create table t3(c1 int,c2 varchar(65536),c3 varchar(65536),c4 varchar(65536));
let $cnt=1;
while($cnt <= 50)
{
  eval insert into t3 (c1,c2,c3,c4) values($cnt,"c2","c3","c4");
  inc $cnt;
}
--enable_query_log
--enable_result_log
update t3 set c2="cc" where c1=2;
delete from t3 where c1=9;

####oracle租户插入数据
connection tentora_conn;
create table t2(c1 int, c2 int, c3 int, primary key(c1));
insert into t2 values(1, 5, 1), (2, 4, 2), (3, 3, 3), (4, 2, 4), (5, 1, 5),(6,2,6),(7,5,7),(8,2,8),(9,3,9);
update t2 set c2=22 where c1=2;
delete from t2 where c1=9;
create table t_test_trg_ora(c1 int);
CREATE OR REPLACE TRIGGER trg_t_test_trg_ora BEFORE INSERT ON t_test_trg_ora FOR EACH ROW BEGIN NULL; END;
source t/compat/include_for_obtest/pl_for_oracle_obdocker.inc;
#不做转储的租户插入数据
connection tentsvr_notminor;
create table notminor(a1 int,a2 VARCHAR2(50),a3 int,primary key(a1));
let $cnt=1;
while($cnt <= 50)
{
  eval insert into notminor (a1,a2,a3) values($cnt,"a_$cnt",$cnt);
  inc $cnt;
}
ALTER TABLE notminor ADD a4 VARCHAR2(50);
ALTER TABLE notminor  MODIFY a4 VARCHAR2(100);
ALTER TABLE notminor  DROP COLUMN a4;
let $cnt=51;
while($cnt <= 70)
{
  eval insert into notminor (a1,a2,a3) values($cnt,"a_$cnt",$cnt);
  inc $cnt;
}
source t/compat/include_for_obtest/transfer_notminor_tent_obdocker.inc;
