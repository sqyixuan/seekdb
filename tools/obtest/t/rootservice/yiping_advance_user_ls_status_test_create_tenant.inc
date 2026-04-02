let $max_used_tenant_id = deploy_get_value(ob1.select select value from __all_sys_stat where name='ob_max_used_tenant_id', value, 1);
let $meta_tenant_id = deploy_get_value(ob1.select select $max_used_tenant_id+1 a, a, 1);
let $user_tenant_id = deploy_get_value(ob1.select select $max_used_tenant_id+2 a, a, 1);
ob1.check_until_timeout select count(*) from gv\$ob_units where unit_id not in (select unit_id from __all_unit) and unit_id in (select unit_id from __all_unit a, __all_resource_pool b where a.resource_pool_id=b.resource_pool_id and b.name='$pool_name'), 0, 120;
ob1.mysql create tenant mysql resource_pool_list=('$pool_name'), primary_zone='$primary_zone', locality='$locality';
# 等3秒，避免某些日志流状态推完了但是选举慢了一步
sleep 3;
let $not_leader_count=deploy_get_value(ob1.select select count(*) a from __all_virtual_ls where tenant_id=$user_tenant_id and ls_id not in (select ls_id from __all_virtual_ls_meta_table where tenant_id=$user_tenant_id and role=1), a, 1);
if ($not_leader_count != 0)
{
  --echo error: some ls has no leader
}
ob1.mysql drop tenant mysql force;
