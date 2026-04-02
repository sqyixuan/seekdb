connection tentsql_conn;
use testsql;
--disable_all_result_log
let $cnt=2001;
while($cnt <= 3000)
{
  eval insert into  t_cg1 (col1,col2) values($cnt,"a_$cnt");
  eval insert into  t_cg2 (col1,col2) values($cnt,"a_$cnt");
  inc $cnt;
}
--enable_all_result_log
