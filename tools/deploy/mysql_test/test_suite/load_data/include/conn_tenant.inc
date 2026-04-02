let $__tenant_name__ = 'tenant_load_data_test';
--source mysql_test/include/get_tenant_server.inc
connect (conn_load_data_test_root, $TE_SERVER_IP, root@tenant_load_data_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_load_data_test_root;
use test;

