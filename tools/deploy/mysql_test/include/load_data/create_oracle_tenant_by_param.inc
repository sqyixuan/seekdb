
let $zone = query_get_value(select zone from oceanbase.dba_ob_zones, zone, 1);
let $unit_num = query_get_value(select count(*) as c from oceanbase.__all_server group by zone, c, 1);

--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS $TENANT_NAME FORCE;
DROP RESOURCE POOL IF EXISTS pool_$TENANT_NAME;
DROP RESOURCE UNIT IF EXISTS uint_$TENANT_NAME;
--enable_warnings

eval CREATE RESOURCE UNIT uint_$TENANT_NAME MAX_CPU=$cpu, MEMORY_SIZE=$memory;
--sleep 5
eval CREATE RESOURCE POOL pool_$TENANT_NAME UNIT='uint_$TENANT_NAME', UNIT_NUM=$unit_num;
eval CREATE TENANT $TENANT_NAME PRIMARY_ZONE = $zone, REPLICA_NUM = 1, RESOURCE_POOL_LIST = ('pool_$TENANT_NAME'), charset = $char_charset set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle', parallel_servers_target=$parallel_servers_target, ob_sql_work_area_percentage=$ob_sql_work_area_percentage, secure_file_priv='/';

eval alter tenant $TENANT_NAME set variables autocommit='on';
eval alter tenant $TENANT_NAME set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
eval alter tenant $TENANT_NAME set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
eval alter tenant $TENANT_NAME set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

--enable_query_log
