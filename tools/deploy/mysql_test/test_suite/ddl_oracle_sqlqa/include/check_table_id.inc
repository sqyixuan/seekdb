--source mysql_test/include/check_schema_sync.inc
connection conn1;

--disable_result_log
SELECT table_id,table_name FROM oceanbase.__all_virtual_table WHERE table_name='Z0CASE';
--enable_result_log

--source mysql_test/include/check_schema_sync.inc
connection zry;
