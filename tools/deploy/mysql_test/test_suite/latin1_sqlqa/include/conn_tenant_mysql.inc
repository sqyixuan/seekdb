let $__tenant_name__ = 'sys';
--source mysql_test/test_suite/latin1_sqlqa/include/get_tenant_server.inc
connect (conn_charset_test_root, $TE_SERVER_IP, root,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_root;

--disable_query_log
#--error 0,1396
--disable_warnings
drop user if exists CHARSET_TEST;
create user CHARSET_TEST identified by '123';
--enable_warnings
grant all on *.* to CHARSET_TEST with grant option;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user,$TE_SERVER_IP,CHARSET_TEST,123,test,$TE_SERVER_PORT);
#connect (conn_charset_test_user,$OBMYSQL_MS0,CHARSET_TEST,123,test,$OBMYSQL_PORT);
connection conn_charset_test_user;

