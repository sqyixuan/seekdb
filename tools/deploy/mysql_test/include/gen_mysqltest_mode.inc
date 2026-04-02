connect (conn_admin_for_gen_mode,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin_for_gen_mode;

let $host_ip = query_get_value(select host_ip() as host_ip from dual, host_ip, 1);
let $rpc_port = query_get_value(select rpc_port() as rpc_port from dual, rpc_port, 1);
let $is_c_mode = query_get_value(select case when with_rootserver = 1 then 1 else 0 end as is_c_mode from oceanbase.__all_server where svr_ip = '$host_ip' and svr_port = $rpc_port, is_c_mode, 1);
let $is_slave_mode = query_get_value(select case when with_rootserver = 0 then 1 else 0 end as is_slave_mode from oceanbase.__all_server where svr_ip = '$host_ip' and svr_port = $rpc_port, is_slave_mode, 1);

connection default;