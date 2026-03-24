let $__tenant_name__ = 'tenant_charset_test';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys, $TE_SERVER_IP, sys@tenant_charset_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys;

--disable_query_log
--error 0,600
drop user lairp123 cascade;
create user lairp123 identified by 123;
grant all on *.* to lairp123 with grant option;
grant dba to lairp123;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user,$TE_SERVER_IP,lairp123@tenant_charset_test,123,LAIRP123,$TE_SERVER_PORT);
connection conn_charset_test_user;


