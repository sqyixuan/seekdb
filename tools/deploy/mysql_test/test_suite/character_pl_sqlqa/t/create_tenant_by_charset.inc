connect (sys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys;

let $zone = query_get_value(select zone from oceanbase.dba_ob_zones, zone, 1);
let $unit_num = query_get_value(select count(*) as c from oceanbase.__all_server group by zone, c, 1);

--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS tenant_charset_test FORCE;
DROP RESOURCE POOL IF EXISTS pool_charset_test;
DROP RESOURCE UNIT IF EXISTS uint_charset_test;
--enable_warnings

eval CREATE RESOURCE UNIT uint_charset_test MAX_CPU=1, MEMORY_SIZE='2G';
eval CREATE RESOURCE POOL pool_charset_test UNIT='uint_charset_test', UNIT_NUM=$unit_num;
eval CREATE TENANT tenant_charset_test PRIMARY_ZONE = $zone, REPLICA_NUM = 1, RESOURCE_POOL_LIST = ('pool_charset_test'), charset = $char_charset set ob_tcp_invited_nodes='%', parallel_servers_target=10, ob_sql_work_area_percentage=20;

alter tenant tenant_charset_test set variables autocommit='on';
alter tenant tenant_charset_test set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant tenant_charset_test set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant tenant_charset_test set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

--enable_query_log

