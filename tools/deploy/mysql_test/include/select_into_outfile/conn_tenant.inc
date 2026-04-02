let $__tenant_name__ = 'tenant_select_into_test';
--source mysql_test/include/get_tenant_server.inc

connect (conn_test_select_into_root, $TE_SERVER_IP, root@tenant_select_into_test,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_test_select_into_root;

create user if not exists admin IDENTIFIED BY 'admin';
--disable_warnings
create database if not exists test;
--disable_warnings
grant all on *.* to admin WITH GRANT OPTION;

connect(conn_test_select_into,$TE_SERVER_IP, admin@tenant_select_into_test, admin, test, $TE_SERVER_PORT);
connection conn_test_select_into;
