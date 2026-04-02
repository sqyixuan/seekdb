#############################################
#owner : cangming.zl
#owner group: rs
#description: quick test
# 测试点：验证server永久下线时server的unit会自动迁走
#############################################
connection conn_sys;

# preprocess
ob1.mysql alter system set server_permanent_offline_time = '20s';

# get server info in z3
let $z3_dst_ip0 = deploy_get_value(ob1.z3.obs0.ip);
let $z3_dst_ip1 = deploy_get_value(ob1.z3.obs1.ip);
let $z3_dst_ip2 = deploy_get_value(ob1.z3.obs2.ip);
let $z3_dst_port0 = deploy_get_value(ob1.z3.obs0.port);
let $z3_dst_port1 = deploy_get_value(ob1.z3.obs1.port);
let $z3_dst_port2 = deploy_get_value(ob1.z3.obs2.port);
let $z3_dst0 = $z3_dst_ip0:$z3_dst_port0;
let $z3_dst1 = $z3_dst_ip1:$z3_dst_port1;
let $z3_dst2 = $z3_dst_ip2:$z3_dst_port2;

# get mysql tenant first unit info in z3
let $resource_pool_id = deploy_get_value(ob1.select SELECT resource_pool_id FROM oceanbase.__all_resource_pool WHERE name = "pool_1", resource_pool_id, 1);
let $unit_id = deploy_get_value(ob1.select SELECT unit_id FROM oceanbase.DBA_OB_UNITS WHERE resource_pool_id = $resource_pool_id AND ZONE = 'z3' ORDER BY unit_id, unit_id, 1);
let $ip = deploy_get_value(ob1.select SELECT svr_ip FROM oceanbase.DBA_OB_UNITS WHERE unit_id = $unit_id, svr_ip, 1);
let $port = deploy_get_value(ob1.select SELECT svr_port FROM oceanbase.DBA_OB_UNITS WHERE unit_id = $unit_id, svr_port, 1);
let $z3_cur = $ip:$port;

# make server of unit_0 permanent offline
--disable_all_result_log
if ($z3_cur == $z3_dst0)
{
  # check unit migrate out
  ob1.z3.obs0.force_stop;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.DBA_OB_UNITS WHERE UNIT_ID = $unit_id AND ((SVR_IP = '$z3_dst_ip1' AND SVR_PORT = $z3_dst_port1) OR (SVR_IP = '$z3_dst_ip2' AND SVR_PORT = $z3_dst_port2)) AND MIGRATE_FROM_SVR_IP IS NULL, 1, 400;
  # restart z3.obs0
  ob1.z3.obs0.start;
}

if ($z3_cur == $z3_dst1)
{
  # check unit migrate out
  ob1.z3.obs1.force_stop;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.DBA_OB_UNITS WHERE UNIT_ID = $unit_id AND ((SVR_IP = '$z3_dst_ip0' AND SVR_PORT = $z3_dst_port0) OR (SVR_IP = '$z3_dst_ip2' AND SVR_PORT = $z3_dst_port2)) AND MIGRATE_FROM_SVR_IP IS NULL, 1, 400;
  # restart z3.obs1
  ob1.z3.obs1.start;
}

if ($z3_cur == $z3_dst2)
{
  # check unit migrate out
  ob1.z3.obs2.force_stop;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.DBA_OB_UNITS WHERE UNIT_ID = $unit_id AND ((SVR_IP = '$z3_dst_ip0' AND SVR_PORT = $z3_dst_port0) OR (SVR_IP = '$z3_dst_ip1' AND SVR_PORT = $z3_dst_port1)) AND MIGRATE_FROM_SVR_IP IS NULL, 1, 400;
  # restart z3.obs2
  ob1.z3.obs2.start;
}
--enable_all_result_log

# check start server success
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where (a.refreshed_schema_version != b.refreshed_schema_version or a.refreshed_schema_version % 8 != 0) and a.tenant_id = 1, 0, 120;
ob1.check_until_timeout SELECT COUNT(*) c FROM DBA_OB_SERVERS WHERE START_SERVICE_TIME > 0, 7, 200;
SELECT COUNT(*) as server_count FROM DBA_OB_SERVERS WHERE STATUS = "ACTIVE";