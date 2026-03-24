let $cpu = 1;
let $memory = '2G';
let $disk = '2G';
let $iops = 1024;
let $session = 64;

connect (sys_conn2,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys_conn2;

--disable_query_log
--disable_warnings
drop tenant if exists dblink_tenant2 force;
drop resource pool if exists dblink_pool2;
drop resource unit if exists dblink_unit2;
--enable_warnings

eval create resource unit dblink_unit2 max_cpu=$cpu,max_iops=$iops,memory_size=$memory;
eval create resource pool if not exists dblink_pool2 unit = 'dblink_unit2', unit_num = 1;
eval create tenant if not exists dblink_tenant2 replica_num = 1, zone_list=('zone1','zone2'), primary_zone= RANDOM, resource_pool_list = ('dblink_pool2') set ob_compatibility_mode='oracle';

alter tenant dblink_tenant2 set variables ob_tcp_invited_nodes='%';
alter tenant dblink_tenant2 set variables autocommit='on';
alter tenant dblink_tenant2 set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant dblink_tenant2 set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant dblink_tenant2 set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
--enable_query_log

--source mysql_test/include/check_tenant_sync.inc
connect (con2,$OBMYSQL_MS0,sys@dblink_tenant2,,*NO-ONE*,$OBMYSQL_PORT);
connection con2;
