connect (sys_conn,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys_conn;

--disable_warnings
--disable_query_log
--error 0,5157
drop tenant yd_tenant force;
drop resource pool if exists yd_pool;
drop resource unit if exists yd_unit;

eval create resource unit if not exists yd_unit max_cpu 2, memory_size '2G';
eval create resource pool if not exists yd_pool unit = 'yd_unit', unit_num = 1;
eval create tenant if not exists yd_tenant resource_pool_list('yd_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
--enable_query_log
--enable_warnings




--source mysql_test/include/check_tenant_sync.inc
connect (con1,$OBMYSQL_MS0,sys@yd_tenant,,*NO-ONE*,$OBMYSQL_PORT);
connection con1;

ALTER SYSTEM SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SYSTEM SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SYSTEM SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';


create user MIC IDENTIFIED BY MIC;
grant all on *.* to MIC WITH GRANT OPTION;
grant dba to MIC;
grant all privileges to MIC;
--source mysql_test/include/check_schema_sync.inc

disconnect sys_conn;
disconnect con1;
