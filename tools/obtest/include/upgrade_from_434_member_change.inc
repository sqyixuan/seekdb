--disable_query_log
connection conn_sys;

let $z3_server_ip = deploy_get_value(ob1.z3.obs0.ip);
let $z3_server_port = deploy_get_value(ob1.z3.obs0.port);
let $z3_server = $z3_server_ip:$z3_server_port;

let $mysql_tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name = 'mysql', tenant_id, 1);

# 等待observer start后，leader切回zone2
ob1.check_until_timeout SELECT COUNT(*) FROM oceanbase.CDB_OB_LS_LOCATIONS WHERE ZONE = 'z2' AND ls_id > 1 AND role = "LEADER" AND tenant_id = $mysql_tenant_id, 1, 30;
let $tenant_ls_id = deploy_get_value(ob1.select SELECT ls_id as ls_id_tmp FROM oceanbase.CDB_OB_LS_LOCATIONS WHERE ZONE = 'z2' AND ls_id > 1 AND role = "LEADER" AND tenant_id = $mysql_tenant_id, ls_id_tmp, 1);

# 测试补副本是否能执行成功
eval ALTER SYSTEM ADD REPLICA LS = $tenant_ls_id SERVER = '$z3_server' REPLICA_TYPE = 'F' PAXOS_REPLICA_NUM = 3 tenant = 'mysql';
ob1.check_until_timeout SELECT COUNT(*) FROM oceanbase.CDB_OB_LS_REPLICA_TASK_HISTORY WHERE tenant_id = $mysql_tenant_id AND ls_id = $tenant_ls_id AND EXECUTE_RESULT like "%OB_SUCCESS%" AND COMMENT like "%add replica by manual%", 1, 30;

# 等待容灾清理新增的副本
ob1.check_until_timeout SELECT COUNT(*) FROM oceanbase.CDB_OB_LS_LOCATIONS WHERE tenant_id = $mysql_tenant_id AND ls_id = $tenant_ls_id AND role = "LEADER" AND member_list LIKE '%$z3_server%', 0, 30;
ob1.check_until_timeout SELECT COUNT(*) FROM oceanbase.CDB_OB_LS_LOCATIONS WHERE tenant_id = $mysql_tenant_id AND ls_id = $tenant_ls_id, 2, 30;
--enable_query_log