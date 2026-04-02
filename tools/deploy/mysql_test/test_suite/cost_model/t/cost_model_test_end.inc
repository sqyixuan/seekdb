--disable_query_log
set _enable_storage_cardinality_estimation = 1;
set optimizer_dynamic_sampling = 1 ;
connection sys;
alter system set_tp tp_no = 566, error_code = 1, frequency = 0;
disconnect sys;
connection default;
--enable_query_log
