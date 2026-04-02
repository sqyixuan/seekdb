connection tentsql_conn;
--disable_all_result_log
use testsql;
eval CREATE TABLE t_cg1 (col1 INT PRIMARY KEY, col2 VARCHAR(50)) WITH COLUMN GROUP(all columns, each column);
eval CREATE TABLE t_cg2 (col1 INT PRIMARY KEY, col2 VARCHAR(50)) WITH COLUMN GROUP(each column);
let $cnt=1;
while($cnt <= 50)
{
  eval insert into  t_cg1 (col1,col2) values($cnt,"a_$cnt");
  eval insert into  t_cg2 (col1,col2) values($cnt,"a_$cnt");
  inc $cnt;
}
--enable_all_result_log
