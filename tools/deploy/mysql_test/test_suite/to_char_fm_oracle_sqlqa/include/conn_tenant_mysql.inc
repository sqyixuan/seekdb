let $__tenant_name__= 'tenant_charset_test_mysql';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys_mysql, $TE_SERVER_IP, root@tenant_charset_test_mysql,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys_mysql;

--disable_query_log
--error 01396
drop user ucharsetmysql;
sleep3;
create user 'ucharsetmysql' indentified by '123';
GRANT all privileges ON *.* TO 'ucharsetmysql'@'%' WITH GRANT OPTION;

--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user_mysql,$OBMYSQL_MS0,ucharsetmysql@tenant_charset_test_mysql,123,test,$OBMYSQL_PORT);
connection conn_charset_test_user_mysql;
