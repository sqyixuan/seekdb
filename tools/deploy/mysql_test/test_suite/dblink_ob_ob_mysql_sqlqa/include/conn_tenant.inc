let $__tenant_name__ = 'tenant_charset_test';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys, $TE_SERVER_IP, root@tenant_charset_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys;

--disable_query_log
use test;
--error 0,1396
drop user ucharset ;
create user 'ucharset' identified by '123';
GRANT all privileges ON *.* TO 'ucharset'@'%' WITH GRANT OPTION;
###grant dba to ucharset;

--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user,$TE_SERVER_IP,ucharset@tenant_charset_test,123,test,$TE_SERVER_PORT);
connection conn_charset_test_user;
