let $zone = query_get_value(select zone from oceanbase.DBA_OB_ZONES, zone, 1);
let $unit_num = query_get_value(select count(*) as c from oceanbase.DBA_OB_SERVERS group by zone, c, 1);

--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS tenant_load_data_test FORCE;
DROP RESOURCE POOL IF EXISTS pool_load_data_test;
DROP RESOURCE UNIT IF EXISTS uint_load_data_test;
--enable_warnings

eval CREATE RESOURCE UNIT uint_load_data_test MAX_CPU=80, MEMORY_SIZE='650G';
--sleep 5
eval CREATE RESOURCE POOL pool_load_data_test UNIT='uint_load_data_test', UNIT_NUM=$unit_num;
eval CREATE TENANT tenant_load_data_test PRIMARY_ZONE = $zone, REPLICA_NUM = 1, RESOURCE_POOL_LIST = ('pool_load_data_test'), charset = $char_charset set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql', parallel_servers_target=10, ob_sql_work_area_percentage=20, secure_file_priv="/";

--enable_query_log
