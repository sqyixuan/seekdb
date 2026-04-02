let $__tenant_name__ = 'sys';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys, $TE_SERVER_IP, root,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys;

#--error 0,1396
--disable_warnings
drop user if exists ucharset ;
create user 'ucharset' identified by 'larp123';
--enable_warnings
grant all privileges on *.* to 'ucharset' with grant option;
--source mysql_test/include/check_schema_sync.inc


connect(conn_charset_test_user,$TE_SERVER_IP,ucharset,larp123,,$TE_SERVER_PORT);
connection conn_charset_test_user;


