let $cpu = 2;
let $memory = '2G';
let $disk = '2G';
let $iops = 1024;
let $session = 64;

connect (sys_conn,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys_conn;

--disable_warnings
--disable_query_log
--error 0,5157
drop tenant my_tenant force;
drop resource pool if exists my_pool;
drop resource unit if exists my_unit;

eval create resource unit my_unit max_cpu=$cpu,max_iops=$iops,memory_size=$memory;
eval create resource pool if not exists my_pool unit = 'my_unit', unit_num = 1;
eval create tenant if not exists my_tenant replica_num = 1, zone_list=('zone1','zone2'), primary_zone= RANDOM, resource_pool_list = ('my_pool') set ob_compatibility_mode='oracle';
--enable_query_log
--enable_warnings

alter tenant my_tenant set variables ob_tcp_invited_nodes='%';
alter tenant my_tenant set variables autocommit='on';
alter tenant my_tenant set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant my_tenant set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant my_tenant set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
alter system set _system_trig_enabled = true tenant my_tenant;

--source mysql_test/include/check_tenant_sync.inc
connect (con1,$OBMYSQL_MS0,sys@my_tenant,,*NO-ONE*,$OBMYSQL_PORT);
connection con1;




