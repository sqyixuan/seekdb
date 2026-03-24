let $cpu = 2;
let $memory = '2G';
let $min_memory = '2G';
let $disk = 536870912;
let $iops = 100000;
let $session = 64;
connect (sys_conn,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys_conn;

--disable_warnings
--disable_query_log
--error 0,5157
drop tenant dbmsjob1_tenant force;
drop resource pool if exists dbmsjob1_pool;
drop resource unit if exists dbmsjob1_unit;

eval create resource unit if not exists dbmsjob1_unit max_cpu 2, memory_size='2G';
eval create resource pool if not exists dbmsjob1_pool unit = 'dbmsjob1_unit', unit_num = 1;
eval create tenant if not exists dbmsjob1_tenant resource_pool_list('dbmsjob1_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
alter system _enable_dbms_job_package=true;
--enable_query_log
--enable_warnings




--source mysql_test/include/check_tenant_sync.inc
connect (con1,$OBMYSQL_MS0,sys@dbmsjob1_tenant,,*NO-ONE*,$OBMYSQL_PORT);
connection con1;

ALTER SYSTEM SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS';
ALTER SYSTEM SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF';
ALTER SYSTEM SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

--echo # 创建测试用户
create user quzq IDENTIFIED BY quzq;
grant all on *.* to quzq WITH GRANT OPTION;
grant dba to quzq;
grant all privileges to quzq;
--source mysql_test/include/check_schema_sync.inc

disconnect con1;
disconnect sys_conn;


