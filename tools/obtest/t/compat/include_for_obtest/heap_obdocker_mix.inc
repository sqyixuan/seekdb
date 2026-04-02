connection tentsql_conn;
use heap_table1;
--disable_all_result_log
let $cnt=600;
while($cnt <= 1000)
{
  eval insert into  test_heap(c1,c2,c3,c4,c5,c6) values (1,1,$cnt,'$cnt',4,'$cnt');
  inc $cnt;
}
--enable_all_result_log
