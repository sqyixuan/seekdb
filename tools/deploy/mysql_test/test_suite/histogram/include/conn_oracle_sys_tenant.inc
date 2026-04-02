let $__tenant_name__ = 'tenant_stat_test';
--source mysql_test/include/get_tenant_server.inc
connect (conn_stat_test_sys, $TE_SERVER_IP, sys@tenant_stat_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_stat_test_sys;

