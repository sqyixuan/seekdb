connection tentsql_conn;
use testsql;
#增量旁路导入
--error 1235,0
insert /*+ direct(true,100,'inc_replace') enable_parallel_dml parallel(2)*/ into tbl1_l select * from t1_1;
#全量旁路导入
delete from tbl2_l;
insert /*+ append enable_parallel_dml parallel(N) */ into tbl2_l select * from tbl1_l;
