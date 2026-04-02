#############################################
#owner : jingyu.cr
#owner group: rs
#description: quick test
# 测试点：验证unit迁移
#############################################
connection conn_sys;

ob1.mysql alter system set debug_sync_timeout = '1000s';
# get mysql's unit info in z3
let $resource_pool_id = deploy_get_value(ob1.select SELECT resource_pool_id FROM oceanbase.__all_resource_pool WHERE name = "pool_1", resource_pool_id, 1);
let $mysql_tenant_id= deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name = 'mysql', tenant_id, 1);
let $unit_id = deploy_get_value(ob1.select SELECT unit_id FROM oceanbase.DBA_OB_UNITS WHERE resource_pool_id = $resource_pool_id AND zone = 'z3', unit_id, 1);
let $ip = deploy_get_value(ob1.select SELECT svr_ip FROM oceanbase.DBA_OB_UNITS WHERE unit_id = $unit_id, svr_ip, 1);
let $port = deploy_get_value(ob1.select SELECT svr_port FROM oceanbase.DBA_OB_UNITS WHERE unit_id = $unit_id, svr_port, 1);
let $z3_cur = $ip:$port;

# get servers' addr in z2, z3
let $z1_dst_ip0 = deploy_get_value(ob1.z1.obs0.ip);
let $z1_dst_ip1 = deploy_get_value(ob1.z1.obs1.ip);
let $z2_dst_ip0 = deploy_get_value(ob1.z2.obs0.ip);
let $z2_dst_ip1 = deploy_get_value(ob1.z2.obs1.ip);
let $z3_dst_ip0 = deploy_get_value(ob1.z3.obs0.ip);
let $z3_dst_ip1 = deploy_get_value(ob1.z3.obs1.ip);
let $z1_dst_port0 = deploy_get_value(ob1.z1.obs0.port);
let $z1_dst_port1 = deploy_get_value(ob1.z1.obs1.port);
let $z2_dst_port0 = deploy_get_value(ob1.z2.obs0.port);
let $z2_dst_port1 = deploy_get_value(ob1.z2.obs1.port);
let $z3_dst_port0 = deploy_get_value(ob1.z3.obs0.port);
let $z3_dst_port1 = deploy_get_value(ob1.z3.obs1.port);
let $z1_dst0 = $z1_dst_ip0:$z1_dst_port0;
let $z1_dst1 = $z1_dst_ip1:$z1_dst_port1;
let $z2_dst0 = $z2_dst_ip0:$z2_dst_port0;
let $z2_dst1 = $z2_dst_ip1:$z2_dst_port1;
let $z3_dst0 = $z3_dst_ip0:$z3_dst_port0;
let $z3_dst1 = $z3_dst_ip1:$z3_dst_port1;

# get sys ls addr
let $sys_ls_obs_z1 = deploy_get_value(ob1.select select concat(svr_ip,":",svr_port) as obs from __all_virtual_core_meta_table where zone = 'z1', obs, 1);
let $sys_ls_obs_z2 = deploy_get_value(ob1.select select concat(svr_ip,":",svr_port) as obs from __all_virtual_core_meta_table where zone = 'z2', obs, 1);

# test migration
ob1.mysql alter system switch replica leader ls = 1 server = '$sys_ls_obs_z1' tenant = 'sys';
ob1.check_until_timeout select count(*) = 1 from oceanbase.DBA_OB_LS_LOCATIONS where role = 'LEADER' and zone = 'z1', 1, 60;
ob1.mysql set ob_global_debug_sync = 'BEFORE_SEND_DRTASK_RPC wait_for signal';
let $ls_count = deploy_get_value(ob1.select SELECT COUNT(*) AS cnt from oceanbase.__all_virtual_ls_meta_table WHERE tenant_id = $mysql_tenant_id AND role = 1, cnt, 1);
if ($z3_cur != $z3_dst0)
{
  --disable_query_log
  eval ALTER SYSTEM MIGRATE UNIT $unit_id DESTINATION "$z3_dst0";
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.__all_rootservice_job WHERE unit_id = $unit_id AND job_type = "MIGRATE_UNIT" AND job_status = "INPROGRESS", 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) > 0 FROM oceanbase.__all_virtual_ls_replica_task, 1, 20;
  ob1.mysql alter system switch replica leader ls = 1 server = '$sys_ls_obs_z2' tenant = 'sys';
  ob1.check_until_timeout select count(*) = 1 from oceanbase.DBA_OB_LS_LOCATIONS where role = 'LEADER' and zone = 'z2', 1, 60;
  ob1.mysql set ob_global_debug_sync = 'now signal signal';
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.DBA_OB_UNITS WHERE UNIT_ID = $unit_id AND MIGRATE_FROM_SVR_IP IS NULL, 1, 400;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.GV\$OB_UNITS WHERE UNIT_ID = $unit_id AND SVR_IP = '$z3_dst_ip0' AND SVR_PORT = $z3_dst_port0 AND TENANT_ID = $mysql_tenant_id - 1, 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.GV\$OB_UNITS WHERE UNIT_ID = $unit_id AND SVR_IP = '$z3_dst_ip0' AND SVR_PORT = $z3_dst_port0 AND TENANT_ID = $mysql_tenant_id, 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) = $ls_count FROM oceanbase.__all_virtual_ls_meta_table WHERE tenant_id = $mysql_tenant_id AND role = 1 AND member_list LIKE '%$z3_dst0%' AND member_list NOT LIKE '%$z3_dst1%', 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.__all_rootservice_job WHERE unit_id = $unit_id AND job_type = "MIGRATE_UNIT" AND job_status = "SUCCESS", 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) AS cnt FROM oceanbase.DBA_OB_UNITS WHERE UNIT_ID = $unit_id AND TENANT_ID = $mysql_tenant_id AND SVR_IP LIKE '%$z3_dst_ip0%' AND SVR_PORT LIKE '%$z3_dst_port0%', 1, 20;
}
if ($z3_cur != $z3_dst1)
{
  --disable_query_log
  eval ALTER SYSTEM MIGRATE UNIT $unit_id DESTINATION "$z3_dst1";
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.__all_rootservice_job WHERE unit_id = $unit_id AND job_type = "MIGRATE_UNIT" AND job_status = "INPROGRESS", 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) > 0 FROM oceanbase.__all_virtual_ls_replica_task, 1, 20;
  ob1.mysql alter system switch replica leader ls = 1 server = '$sys_ls_obs_z2' tenant = 'sys';
  ob1.check_until_timeout select count(*) = 1 from oceanbase.DBA_OB_LS_LOCATIONS where role = 'LEADER' and zone = 'z2', 1, 60;
  ob1.mysql set ob_global_debug_sync = 'now signal signal';
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.DBA_OB_UNITS WHERE UNIT_ID = $unit_id AND MIGRATE_FROM_SVR_IP IS NULL, 1, 400;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.GV\$OB_UNITS WHERE UNIT_ID = $unit_id AND SVR_IP = '$z3_dst_ip1' AND SVR_PORT = $z3_dst_port1 AND TENANT_ID = $mysql_tenant_id - 1, 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.GV\$OB_UNITS WHERE UNIT_ID = $unit_id AND SVR_IP = '$z3_dst_ip1' AND SVR_PORT = $z3_dst_port1 AND TENANT_ID = $mysql_tenant_id, 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) = $ls_count FROM oceanbase.__all_virtual_ls_meta_table WHERE tenant_id = $mysql_tenant_id AND role = 1 AND member_list LIKE '%$z3_dst1%' AND member_list NOT LIKE '%$z3_dst0%', 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) = 1 FROM oceanbase.__all_rootservice_job WHERE unit_id = $unit_id AND job_type = "MIGRATE_UNIT" AND job_status = "SUCCESS", 1, 20;
  ob1.check_until_timeout SELECT COUNT(*) AS cnt FROM oceanbase.DBA_OB_UNITS WHERE UNIT_ID = $unit_id AND TENANT_ID = $mysql_tenant_id AND SVR_IP LIKE '%$z3_dst_ip1%' AND SVR_PORT LIKE '%$z3_dst_port1%', 1, 20;
}

