--disable_query_log
--error 0,600
drop user whh_dblink cascade;
create user whh_dblink identified by 123;
grant all on *.* to whh_dblink with grant option;
grant dba to whh_dblink;
--source mysql_test/include/check_schema_sync.inc
--enable_query_log

connect(conn_dblink_tenant2,$OBMYSQL_MS0,whh_dblink@dblink_tenant2,123,whh_dblink,$OBMYSQL_PORT);
connection conn_dblink_tenant2;
