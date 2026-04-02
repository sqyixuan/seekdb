let $zone = query_get_value(select zone from oceanbase.DBA_OB_ZONES, zone, 1);
let $unit_num = query_get_value(select count(*) as c from oceanbase.DBA_OB_SERVERS group by zone, c, 1);

--disable_query_log
--disable_warnings
drop tenant if exists tenant_compat_test FORCE;
drop resource pool if exists pool_compat_test;
drop resource unit if exists uint_compat_test;
--enable_warnings

eval CREATE RESOURCE UNIT uint_compat_test MAX_CPU=1, MEMORY_SIZE='2G';
eval CREATE RESOURCE POOL pool_compat_test UNIT='uint_compat_test', UNIT_NUM=$unit_num;
eval CREATE TENANT tenant_compat_test PRIMARY_ZONE = $zone, REPLICA_NUM = 1, RESOURCE_POOL_LIST = ('pool_compat_test') set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql', ob_compatibility_control=$compat_type, $version_name=$version_value;
--enable_query_log
