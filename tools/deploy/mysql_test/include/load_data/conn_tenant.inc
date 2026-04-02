let $__tenant_name__ = '$TENANT_NAME';
--source mysql_test/include/get_tenant_server.inc
connect (conn_sys_user, $TE_SERVER_IP, sys@$TENANT_NAME,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_sys_user;

--disable_query_log
--error 0,600
eval drop user $USER_NAME cascade;
eval create user $USER_NAME identified by $PASSWORD;
eval grant all on *.* to $USER_NAME with grant option;
eval grant dba to $USER_NAME;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_benchmark_user,$TE_SERVER_IP,$USER_NAME@$TENANT_NAME,$USER_NAME,$PASSWORD,$TE_SERVER_PORT);
connection conn_benchmark_user;


