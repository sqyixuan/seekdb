let $__tenant_name__ = 'tenant_charset_test_mysql';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys_mysql, $TE_SERVER_IP, root@tenant_charset_test_mysql,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys_mysql;

--disable_query_log
--error 0,1396
drop user ucharsemysql;
create user 'ucharsemysqltest' identified by '123';

--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_charset_test_user_mysqltest,$TE_SERVER_IP,ucharsemysqltest@tenant_charset_test_mysql,123,test,$TE_SERVER_PORT);
connection conn_charset_test_user_mysqltest;

