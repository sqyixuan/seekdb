connection tentsql_conn;
--disable_all_result_log
use testsql;
let $cnt=1001;
while($cnt <= 2000)
{
  eval insert into  t_cg1 (col1,col2) values($cnt,"a_$cnt");
  eval insert into  t_cg2 (col1,col2) values($cnt,"a_$cnt");
  inc $cnt;
}
--enable_all_result_log
