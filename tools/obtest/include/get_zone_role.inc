let $zone_user_table_leader_count = 0;
let $leader_count = deploy_get_value(ob1.select select count(*) c from __all_virtual_ls_meta_table  where zone = '$zone' and role = 1, c, 1);
if ($leader_count != 0)
{
  inc $zone_user_table_leader_count;
}
let $zone_sys_table_leader_count = deploy_get_value(ob1.select select count(*) c from __all_virtual_core_meta_table where zone = '$zone' and role = 1, c, 1);
let $zone_leader_count = $zone_user_table_leader_count:$zone_sys_table_leader_count;

let $zone_user_table_follower_count = 0;
let $follower_count = deploy_get_value(ob1.select select count(*) c from __all_virtual_ls_meta_table where zone = '$zone' and role = 2, c, 1);
if ($follower_count != 0)
{
  inc $zone_user_table_follower_count;
}
let $zone_sys_table_follower_count = deploy_get_value(ob1.select select count(*) c from __all_virtual_core_meta_table where zone = '$zone' and role = 2, c, 1);
let $zone_follower_count = $zone_user_table_follower_count:$zone_sys_table_follower_count;
if ($zone_leader_count == 0:0)
{
  if ($zone_follower_count != 0:0)
  {
    echo zone is follower;
  }
}

if ($zone_follower_count == 0:0)
{
  if ($zone_leader_count != 0:0)
  {
    echo zone is leader;
  }
}
