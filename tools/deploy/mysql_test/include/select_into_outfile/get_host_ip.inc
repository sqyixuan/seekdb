connect (obsys_for_select_into,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_for_select_into;

--disable_query_log
let $HOST_IP = query_get_value('select host_ip()',host_ip(), 1);
--enable_query_log

disconnect obsys_for_select_into;
connection default;

