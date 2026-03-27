# set __table_name__ outside
# set __start_time__ outside
# set __finish_time__ outside

--disable_query_log
--disable_result_log

connect (obsys_minor,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_minor;

let $__result__ = query_get_value(select count(*) from oceanbase.__all_virtual_server_event_history where gmt_create >= unix_timestamp('$__start_time__') * 1000000 and gmt_create <= unix_timestamp('$__finish_time__') * 1000000 and module = 'direct_load' and event = 'switch_status' and value5 like $__table_name__, count(*), 1);
--echo $__result__

disconnect obsys_minor;
connection default;

--enable_query_log
--enable_result_log
