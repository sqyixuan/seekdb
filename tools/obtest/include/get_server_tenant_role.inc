let $tenant_id = deploy_get_value(ob1.select select tenant_id from __all_tenant where tenant_name = '$tenant_name', tenant_id, 1);
let $server_tenant_user_table_leader_count = deploy_get_value(ob1.select select count(*) c from __all_virtual_meta_table where svr_ip = '$ip' and svr_port = $port and tenant_id = $tenant_id and role = 1, c, 1);
let $server_tenant_sys_table_leader_count = deploy_get_value(ob1.select select count(*) c from __all_root_table where svr_ip = '$ip' and svr_port = $port and tenant_id = $tenant_id and role = 1, c, 1);
let $server_tenant_leader_count = $server_tenant_user_table_leader_count:$server_tenant_sys_table_leader_count;

let $server_tenant_user_table_follower_count = deploy_get_value(ob1.select select count(*) c from __all_virtual_meta_table where svr_ip = '$ip' and svr_port = $port and tenant_id = $tenant_id and role = 2, c, 1);
let $server_tenant_sys_table_follower_count = deploy_get_value(ob1.select select count(*) c from __all_root_table where svr_ip = '$ip' and svr_port = $port and tenant_id = $tenant_id and role = 2, c, 1);
let $server_tenant_follower_count = $server_tenant_user_table_follower_count:$server_tenant_sys_table_follower_count;
if ($server_tenant_leader_count == 0:0)
{
  if ($server_tenant_follower_count != 0:0)
  {
    echo server tenant is follower;
  }
}

if ($server_tenant_follower_count == 0:0)
{
  if ($server_tenant_leader_count != 0:0)
  {
    echo server tenant is leader;
  }
}
