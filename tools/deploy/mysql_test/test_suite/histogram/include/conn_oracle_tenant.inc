let $__tenant_name__ = 'tenant_stat_test';
--source mysql_test/include/get_tenant_server.inc
connect (conn_stat_test_sys, $TE_SERVER_IP, sys@tenant_stat_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_stat_test_sys;

--disable_query_log
--error 0,600
drop user ustat cascade;
create user ustat identified by 123;
grant all on *.* to ustat with grant option;
grant dba to ustat;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_stat_test_user,$TE_SERVER_IP,USTAT@tenant_stat_test,123,USTAT,$TE_SERVER_PORT);
connection conn_stat_test_user;


