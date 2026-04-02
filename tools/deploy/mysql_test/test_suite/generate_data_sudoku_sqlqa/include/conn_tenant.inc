let $__tenant_name__ = 'tenant_yd';
--source mysql_test/include/get_tenant_server.inc
connect (conn_sd_test_sys, $TE_SERVER_IP, sys@tenant_yd,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_sd_test_sys;

--disable_query_log
--error 0,600
drop user MIC cascade;
create user MIC identified by MIC;
grant all on *.* to MIC with grant option;
grant dba to MIC;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log
connect(conn,$OBMYSQL_MS0,MIC@tenant_yd,MIC,MIC,$OBMYSQL_PORT);
connection conn;


