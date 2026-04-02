--disable_query_log
connect(sys,$OBMYSQL_MS0,root@sys,,oceanbase,$OBMYSQL_PORT);
connection sys;
--echo #########################
--echo ###  默认只展示0号算子的op cost
--echo #########################
alter system set_tp tp_no = 566, error_code = 1, frequency = 1;
connection default;
set _enable_storage_cardinality_estimation = 0;
set optimizer_dynamic_sampling = 0 ;
--enable_query_log
