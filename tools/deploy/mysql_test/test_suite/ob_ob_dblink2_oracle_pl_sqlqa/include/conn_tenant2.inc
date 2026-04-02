--disable_query_log
--error 0,600
drop user whh_dblink2 cascade;
create user whh_dblink2 identified by 123;
grant all on *.* to whh_dblink2 with grant option;
grant dba to whh_dblink2;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_dblink_tenant2,$OBMYSQL_MS0,whh_dblink2@dblink_tenant2,123,whh_dblink2,$OBMYSQL_PORT);
connection conn_dblink_tenant2;
