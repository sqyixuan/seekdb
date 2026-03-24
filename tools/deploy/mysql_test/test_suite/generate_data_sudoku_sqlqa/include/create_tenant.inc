connect (sys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys;

let $zone = query_get_value(select zone from oceanbase.DBA_OB_ZONES, zone, 1);
let $unit_num = query_get_value(select count(*) as c from oceanbase.DBA_OB_SERVERS group by zone, c, 1);

--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS tenant_yd FORCE;
DROP RESOURCE POOL IF EXISTS pool_yd;
DROP RESOURCE UNIT IF EXISTS uint_yd;
--enable_warnings

eval create resource unit  uint_yd max_cpu 2, memory_size='2G';
eval create resource pool  pool_yd unit = 'uint_yd', unit_num = $unit_num;
eval create tenant tenant_yd PRIMARY_ZONE = $zone, resource_pool_list('pool_yd') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';


alter tenant tenant_yd set variables autocommit='on';
alter tenant tenant_yd set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant tenant_yd set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant tenant_yd set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

--enable_query_log
