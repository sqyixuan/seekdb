connect (sys_conn,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys_conn;

--disable_warnings
--disable_query_log
--error 0,5157
#drop tenant cwx_tenant force;
#drop resource pool if exists cwx_pool;
#drop resource unit if exists cwx_unit;

#eval create resource unit if not exists cwx_unit max_cpu 2, memory_size '2G';
#eval create resource pool if not exists cwx_pool unit = 'cwx_unit', unit_num = 1;
#eval create tenant if not exists cwx_tenant resource_pool_list('cwx_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql';
--enable_query_log
--enable_warnings




--source mysql_test/include/check_tenant_sync.inc
connect (con1,$OBMYSQL_MS0,root,,*NO-ONE*,$OBMYSQL_PORT);
connection con1;

#alter tenant cwx_tenant set variables autocommit='on';
#alter tenant cwx_tenant set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
#alter tenant cwx_tenant set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
#alter tenant cwx_tenant set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';


--echo # 创建测试用户
--disable_warnings
drop database if exists cwx_db;
drop database if exists cwx_db1;
drop user if exists cwx;
drop user if exists cwx1;
--enable_warnings
create database cwx_db;
use cwx_db;
create user 'cwx' identified by 'cwx';
grant all privileges on *.* to 'cwx';
create database cwx_db1;
use cwx_db1;
create user 'cwx1' identified by 'cwx1';
grant all privileges on *.* to 'cwx1';

disconnect con1;
disconnect sys_conn;


