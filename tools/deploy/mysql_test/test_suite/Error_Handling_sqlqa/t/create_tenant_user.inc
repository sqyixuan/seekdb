let $cpu = 1;
let $memory = '2G';
let $disk = '5G';
let $iops = 1024;
connect (sys_conn,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys_conn;

--disable_warnings
--disable_query_log
#--error 0,5157
#drop tenant dbmsjob1_tenant force;
#drop resource pool if exists dbmsjob1_pool;
#drop resource unit if exists dbmsjob1_unit;

#eval create resource unit if not exists dbmsjob1_unit max_cpu $cpu, MEMORY_SIZE $memory, max_iops $iops, LOG_DISK_SIZE $disk, MIN_CPU= $cpu, MIN_IOPS=$iops;
#eval create resource pool if not exists dbmsjob1_pool unit = 'dbmsjob1_unit', unit_num = 1;
#eval create tenant if not exists dbmsjob1_tenant resource_pool_list('dbmsjob1_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql';
--enable_query_log
--enable_warnings

#alter tenant dbmsjob1_tenant set variables autocommit='on';
#alter tenant dbmsjob1_tenant set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
#alter tenant dbmsjob1_tenant set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
#alter tenant dbmsjob1_tenant set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';


--source mysql_test/include/check_tenant_sync.inc
connect (con1,$OBMYSQL_MS0,root,,*NO-ONE*,$OBMYSQL_PORT);
connection con1;


--echo # 创建测试用户
--disable_warnings
drop user if exists 'funkzz';
create user 'funkzz' IDENTIFIED BY '123456';
--enable_warnings
--disable_warnings
drop database if exists zh_db;
--enable_warnings
create database if not exists  zh_db;
grant all on *.* to 'funkzz' WITH GRANT OPTION;
grant all privileges ON *.* TO 'funkzz'@'%' IDENTIFIED BY '123456' WITH GRANT OPTION;
--source mysql_test/include/check_schema_sync.inc

disconnect con1;
disconnect sys_conn;





