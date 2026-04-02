connection tentsql_conn;
use testsql;
#全量旁路导入
delete from tbl2_l;
insert /*+ append enable_parallel_dml parallel(N) */ into tbl2_l select * from tbl1_l;
