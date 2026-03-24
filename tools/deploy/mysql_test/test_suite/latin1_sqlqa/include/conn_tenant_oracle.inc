let $__tenant_name__ = 'tenant_charset_test';
--source mysql_test/test_suite/hkscs_utf16le_sqlqa/include/get_tenant_server.inc
connect (conn_charset_test_sys, $TE_SERVER_IP, sys@tenant_charset_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys;

--disable_query_log
--error 0,600
drop user charset_test cascade;
create user charset_test identified by 123;
grant all on *.* to charset_test with grant option;
grant dba,resource,connect to charset_test;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user,$TE_SERVER_IP,charset_test@tenant_charset_test,123,SYS,$TE_SERVER_PORT);
connection conn_charset_test_user;




