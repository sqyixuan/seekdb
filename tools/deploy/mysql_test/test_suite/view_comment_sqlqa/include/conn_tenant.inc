let $__tenant_name__ = 'gbk_tnt1';
--source mysql_test/include/get_tenant_server.inc
connect (conn_charset_test_sys1, $TE_SERVER_IP, sys@gbk_tnt1,,*NO-ONE*, $TE_SERVER_PORT);
connection conn_charset_test_sys1;

--disable_query_log

--error 0,600
drop user monkey cascade;
create user monkey IDENTIFIED BY monkey;
grant all on *.* to monkey WITH GRANT OPTION;
grant dba to monkey;

--error 0,600
drop user yyl_dblink cascade;
create user yyl_dblink identified by 123;
grant all on *.* to yyl_dblink with grant option;
grant dba to yyl_dblink;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_gbk_tnt1,$TE_SERVER_IP,monkey@gbk_tnt1,monkey,monkey,$TE_SERVER_PORT);
connection conn_gbk_tnt1;
