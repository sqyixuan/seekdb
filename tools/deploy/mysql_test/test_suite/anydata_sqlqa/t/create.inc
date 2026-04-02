#connect (sys_conn,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
#connection sys_conn;

--disable_warnings
--disable_query_log
#sleep 15;
--error 0,5157
#drop tenant cwx_tenant force;
--error 0,4624
#drop resource pool if exists cwx_pool;
--error 0,5157
#drop resource unit if exists cwx_unit;
#eval create resource unit if not exists cwx_unit max_cpu 2, memory_size '2G';
#eval create resource pool if not exists cwx_pool unit = 'cwx_unit', unit_num = 1;
#eval create tenant if not exists cwx_tenant resource_pool_list('cwx_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';



#--source mysql_test/include/check_tenant_sync.inc
#connect (con1,$OBMYSQL_MS0,sys@cwx_tenant,,*NO-ONE*,$OBMYSQL_PORT);
#connection con1;

#ALTER SYSTEM SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
#ALTER SYSTEM SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
#ALTER SYSTEM SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

--echo # 创建测试用户
--error 0,600
drop user cwx cascade;
create user cwx IDENTIFIED BY cwx;
grant all on *.* to cwx WITH GRANT OPTION;
grant dba to cwx;
grant all privileges to cwx;
#--source mysql_test/include/check_schema_sync.inc

#disconnect con1;
#disconnect sys_conn;


--enable_query_log
--enable_warnings


