let $__tenant_name__ = 'dblink_tnt1';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys1, $TE_SERVER_IP, sys@dblink_tnt1,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys1;

--disable_query_log
--error 0,600
drop user yyl_dblink cascade;
create user yyl_dblink identified by 123;
grant all on *.* to yyl_dblink with grant option;
grant dba to yyl_dblink;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_dblink_tnt1,$TE_SERVER_IP,yyl_dblink@dblink_tnt1,123,yyl_dblink,$TE_SERVER_PORT);
connection conn_dblink_tnt1;
