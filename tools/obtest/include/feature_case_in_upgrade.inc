#####################
# for ash-pl test
#####################

connection conn_sys;
--disable_query_log
--disable_result_log
call dbms_workload_repository.ash_report(str_to_date('2023-10-31 00:00:00', '%Y-%m-%d %H:%i:%s'),CURRENT_TIMESTAMP);
--enable_result_log
--enable_query_log

connection conn_sys;
