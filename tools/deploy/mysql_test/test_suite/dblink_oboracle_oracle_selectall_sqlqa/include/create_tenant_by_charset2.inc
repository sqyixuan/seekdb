let $cpu = 1;
let $memory = '1G';
let $disk = '5G';
let $iops = 128;
let $session = 64;

###connect (sys2,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys2;

let $zone = query_get_value(select zone from oceanbase.dba_ob_zones, zone, 1);
let $unit_num = query_get_value(select count(*) as c from oceanbase.__all_server group by zone, c, 1);

--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS dblink_tnt2 FORCE;
DROP RESOURCE POOL IF EXISTS pool_charset_test2;
DROP RESOURCE UNIT IF EXISTS uint_charset_test2;
--enable_warnings

eval CREATE RESOURCE UNIT uint_charset_test2 MAX_CPU=1, MEMORY_SIZE='2G';
eval CREATE RESOURCE POOL pool_charset_test2 UNIT='uint_charset_test2', UNIT_NUM=$unit_num;
eval CREATE TENANT dblink_tnt2 PRIMARY_ZONE = $zone, REPLICA_NUM = 1, RESOURCE_POOL_LIST = ('pool_charset_test2'), charset = $char_charset set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle', parallel_servers_target=10, ob_sql_work_area_percentage=20;

alter tenant dblink_tnt2 set variables autocommit='on';
alter tenant dblink_tnt2 set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant dblink_tnt2 set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant dblink_tnt2 set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

--enable_query_log

