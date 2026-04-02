connection tentsql_conn;
use testsql;
#增量旁路导入
CREATE TABLE tbl1_l (col1 BIGINT PRIMARY KEY, col2 VARCHAR(50)) PARTITION BY LIST (col1) (PARTITION p0 VALUES IN (1, 2, 3), PARTITION p1 VALUES IN (5, 6), PARTITION p2 VALUES IN (DEFAULT));
insert /*+ direct(true,100,'inc_replace') enable_parallel_dml parallel(2)*/ into tbl1_l select * from t1;
#全量旁路导入
CREATE TABLE tbl2_l (col1 BIGINT PRIMARY KEY, col2 VARCHAR(50)) PARTITION BY LIST (col1) (PARTITION p0 VALUES IN (1, 2, 3), PARTITION p1 VALUES IN (5, 6), PARTITION p2 VALUES IN (DEFAULT));
insert /*+ direct enable_parallel_dml parallel(N) */ into tbl2_l select * from tbl1_l;
create table t1_1(c1 int, c2 int);
insert into t1_1 values(11, 1), (12, 2), (13, 3), (14, 4), (15, 5), (16, 6), (17, 7), (18, 8), (19, 9), (20, 10);
create table t1_2(c1 int, c2 int);
insert into t1_2 values(21, 1), (22, 2), (23, 3), (24, 4), (25, 5), (26, 6), (27, 7), (28, 8), (29, 9), (30, 10);
