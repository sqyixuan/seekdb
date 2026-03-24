--disable_query_log
--disable_result_log
--connect (sys,$OBMYSQL_MS0,root@sys,,,$OBMYSQL_PORT)
--error 0,1210
alter system set_tp tp_no = 562, error_code = 4016, frequency = 1;
connection default;
--enable_query_log
--enable_result_log
