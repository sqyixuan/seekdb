let $__tenant_name__ = 'sys';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys_mysql, $TE_SERVER_IP, root,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys_mysql;

--disable_query_log
--error 0,1396
drop user ucharsetmysql;
sleep 1;
create user 'ucharsetmysql' identified by '123';
GRANT all privileges ON *.* TO 'ucharsetmysql'@'%' WITH GRANT OPTION;

--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user_mysql,$OBMYSQL_MS0,ucharsetmysql,123,test,$OBMYSQL_PORT);
connection conn_charset_test_user_mysql;

