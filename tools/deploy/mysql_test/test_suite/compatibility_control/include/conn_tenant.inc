let $__tenant_name__ = 'sys';
--disable_query_log
--source mysql_test/include/get_tenant_server.inc
connect (conn_compat_test_root, $TE_SERVER_IP, root,,test, $TE_SERVER_PORT);
connection conn_compat_test_root;
--enable_query_log

