--disable_query_log
--error 0,600
drop user whh_dblink1 cascade;
create user whh_dblink1 identified by 123;
grant all on *.* to whh_dblink1 with grant option;
grant dba to whh_dblink1;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_dblink_tenant1,$OBMYSQL_MS0,whh_dblink1@dblink_tenant1,123,whh_dblink1,$OBMYSQL_PORT);
connection conn_dblink_tenant1;
