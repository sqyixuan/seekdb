
#mysql租户+仲裁租户插入数据
connection tentsql_conn;
use testsvr;
create table t_part1(c1 int, c2 int) partition by hash(c1) partitions 8;
insert into t_part1 values(1, 1);

CREATE TABLEGROUP Tablegroup1;
CREATE TABLEGROUP Tablegroup2 SHARDING="PARTITION";
CREATE TABLEGROUP Tablegroup3 SHARDING="NONE";

CREATE TABLEGROUP Tablegroup1_1;
CREATE TABLEGROUP Tablegroup2_1 SHARDING="PARTITION";
CREATE TABLEGROUP Tablegroup3_1 SHARDING="NONE";

CREATE TABLEGROUP Tablegroup1_2;
CREATE TABLEGROUP Tablegroup2_2 SHARDING="PARTITION";
CREATE TABLEGROUP Tablegroup3_2 SHARDING="NONE";

ALTER TABLEGROUP Tablegroup1_2 ADD TABLE testsql.t_part;
ALTER TABLEGROUP Tablegroup2_2 ADD TABLE testsql.t_part;
ALTER TABLEGROUP Tablegroup3_2 ADD TABLE testsql.t1;

source t/compat/include_for_obtest/create_user_obdocker.inc;
