let $__tenant_name__ = 'sys';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys, $TE_SERVER_IP, root,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys;

--disable_query_log
--error 0,600,1064
drop user ucharset;
create user ucharset identified by '123';
grant all on *.* to ucharset with grant option;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user,$TE_SERVER_IP,ucharset,123,test,$TE_SERVER_PORT);
connection conn_charset_test_user;


