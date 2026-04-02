ob_connect(conn_switch_replica_leader, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_switch_replica_leader;
--disable_query_log
let $tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name = $tenant_name, tenant_id, 1);
let $leader_ip_port=deploy_get_value(ob1.select select concat(svr_ip,':',svr_port) as host from __all_virtual_ls_meta_table where tenant_id = $tenant_id and ls_id = $ls_id and role = 1, host, 1);
let $leader_svr_ip = deploy_get_value(ob1.select select svr_ip from __all_virtual_ls_meta_table where tenant_id = $tenant_id and ls_id = $ls_id and role = 1, svr_ip, 1);
let $leader_svr_port = deploy_get_value(ob1.select select svr_port from __all_virtual_ls_meta_table where tenant_id = $tenant_id and ls_id = $ls_id and role = 1, svr_port, 1);
let $follower_svr_ip = deploy_get_value(ob1.select select svr_ip from __all_virtual_ls_meta_table where tenant_id = $tenant_id and ls_id = $ls_id and role = 2, svr_ip, 1);
let $follower_svr_port = deploy_get_value(ob1.select select svr_port from __all_virtual_ls_meta_table where tenant_id = $tenant_id and ls_id = $ls_id and role = 2, svr_port, 1);

eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$tenant_id' and svr_ip = '$follower_svr_ip' and svr_port='$follower_svr_port' and ls_id = $ls_id and role = 1;
eval alter system switch replica leader ls=$ls_id server='$follower_svr_ip:$follower_svr_port' tenant=$tenant_name;
sleep 10;
source include/liboblog/check_schema_fast_sync.inc;

eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$tenant_id' and svr_ip = '$follower_svr_ip' and svr_port='$follower_svr_port' and ls_id = $ls_id and role = 1;
--enable_query_log
