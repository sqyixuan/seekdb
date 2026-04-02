let $__tenant_name__ = 'tenant_charset_test';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys, $TE_SERVER_IP, sys@tenant_charset_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys;

