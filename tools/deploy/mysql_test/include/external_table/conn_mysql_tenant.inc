let $__tenant_name__ = '$TENANT_NAME';
--source mysql_test/include/get_tenant_server.inc
connect (conn_sys_user, $TE_SERVER_IP, root@$TENANT_NAME,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_sys_user;

--disable_query_log
--error 0,1396
eval drop user $USER_NAME;
eval create user $USER_NAME identified by '$PASSWORD';
--disable_warnings
eval create database if not exists $USER_NAME;
--disable_warnings
eval grant all on *.* to $USER_NAME with grant option;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_benchmark_user,$TE_SERVER_IP,$USER_NAME@$TENANT_NAME,$USER_NAME,$PASSWORD,$TE_SERVER_PORT);
connection conn_benchmark_user;


