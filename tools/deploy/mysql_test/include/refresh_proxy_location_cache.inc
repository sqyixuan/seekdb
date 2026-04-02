--disable_query_log
--disable_result_log

connect (obsys_minor,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_minor;

sleep 2;

disconnect obsys_minor;
connection default;

--enable_query_log
--enable_result_log
