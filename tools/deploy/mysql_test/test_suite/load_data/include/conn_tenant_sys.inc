let $__tenant_name__ = 'tenant_load_data';
--source mysql_test/include/get_tenant_server.inc
connect (conn_load_data_sys, $TE_SERVER_IP, sys@tenant_load_data_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_load_data_sys;

