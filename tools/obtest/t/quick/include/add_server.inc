#############################################
#owner : jingyu.cr
#owner group: rs
#description: quick test
# 测试点：增加server数量, 从1:1:1到2:2:2
#############################################
SELECT COUNT(*) as server_count FROM DBA_OB_SERVERS WHERE STATUS = "ACTIVE";
SELECT COUNT(*) as server_count FROM GV$OB_SERVERS;
ob1.z1.obs1.set extra_config=$extra_conf;
ob1.z2.obs1.set extra_config=$extra_conf;
ob1.z3.obs1.set extra_config=$extra_conf;
ob1.z1.obs1.set status=enable;
ob1.z2.obs1.set status=enable;
ob1.z3.obs1.set status=enable;
ob1.z1.obs1.start;
ob1.z2.obs1.start;
ob1.z3.obs1.start;

let $tenant_cnt = 1;
let $server_cnt = 6;
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where (a.refreshed_schema_version != b.refreshed_schema_version or a.refreshed_schema_version % 8 != 0) and a.tenant_id = 1, 0, 120;
ob1.z1.obs1.check_log_until_success -c "weak read service thread start";
ob1.z2.obs1.check_log_until_success -c "weak read service thread start";
ob1.z3.obs1.check_log_until_success -c "weak read service thread start";
ob1.z1.obs1.add;
ob1.z2.obs1.add;
ob1.z3.obs1.add;
ob1.check_until_timeout SELECT COUNT(*) c FROM DBA_OB_SERVERS WHERE START_SERVICE_TIME > 0, 6, 200;
set @@session.ob_query_timeout = 100000000000;
SELECT COUNT(*) as server_count FROM DBA_OB_SERVERS WHERE STATUS = "ACTIVE";
ob1.check_until_timeout SELECT COUNT(*) as server_count FROM GV\$OB_SERVERS, 6, 8;
ob1.check_until_timeout select IF(count(*) = $tenant_cnt * $server_cnt, 1, 0) as done from oceanbase.__all_virtual_server_schema_info, 1, 120;
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where (a.refreshed_schema_version != b.refreshed_schema_version or a.refreshed_schema_version % 8 != 0) and a.tenant_id <= $mysql_tenant_id, 0, 60;
