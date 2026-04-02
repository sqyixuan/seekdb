connection tentsql_conn;
use heap_table1;
--disable_all_result_log
let $cnt=1600;
while($cnt <= 2000)
{
  eval insert into  test_heap(c1,c2,c3,c4,c5,c6) values (1,1,$cnt,'$cnt',4,'$cnt');
  inc $cnt;
}
--enable_all_result_log
ALTER TABLE test_heap drop c6;
ALTER TABLE test_heap ADD c6 varchar(80);
rename table test_heap to test_heap1;
ALTER TABLE test_heap1 MODIFY COLUMN c4 int;
--disable_all_result_log
let $cnt=2600;
while($cnt <= 3000)
{
  eval insert into  test_heap1(c1,c2,c3,c4,c5,c6) values (1,1,$cnt,'$cnt',4,'$cnt');
  inc $cnt;
}
--enable_all_result_log
TRUNCATE TABLE test_heap1;
drop table test_heap1;
