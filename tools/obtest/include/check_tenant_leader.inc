#usage: $version

let $obs = deploy_get_value(ob1.z1.obs0.ip);
let $port = deploy_get_value(ob1.z1.obs0.mysql_port);
connect (conn1,$obs,root,,oceanbase,$port);
connection conn1;

let $cnt = 360;
while ($cnt > 0)
{
  let $merge = deploy_get_value(ob1.select select count(*) c from oceanbase.__all_zone where name ="last_merged_version" and value = $version and zone = '', c, 1);
  if ($merge == 1)
  {
    let $cnt = 0;
  }
  #在合并的zone不能有主存在；
  let $leader = deploy_get_value(ob1.select select  /*+ READ_CONSISTENCY(WEAK) */ count(*) c from oceanbase.__all_zone, oceanbase.__all_virtual_meta_table where role = 1 and oceanbase.__all_zone.zone = oceanbase.__all_virtual_meta_table.zone  and oceanbase.__all_zone.name = "merge_status" and oceanbase.__all_zone.info="MERGING", c, 1);
  if ($leader > 1)
  {
    echo 'merging zone has leader. failed';
    select  /*+ READ_CONSISTENCY(WEAK) */ count(*) c from oceanbase.__all_zone, oceanbase.__all_virtual_meta_table where role = 1 and oceanbase.__all_zone.zone = oceanbase.__all_virtual_meta_table.zone  and oceanbase.__all_zone.name = "merge_status" and oceanbase.__all_zone.info="MERGING";
    select * from __all_zone;
    select * from __all_virtual_meta_table where role = 1;
  }
  #主不能切到其他region
  let $tenant_cnt = deploy_get_value(ob1.select select count(*) c from oceanbase.__all_tenant, c, 1);
  dec $tenant_cnt;
  while ($tenant_cnt >=0) 
  {
    let $primary_zone = deploy_get_value(ob1.select select primary_zone from oceanbase.__all_tenant limit 1 offset $tenant_cnt, primary_zone, 1); 
    let $tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant limit 1 offset $tenant_cnt, tenant_id, 1);
    if ($primary_zone == z3 || $primary_zone == z4)
    {
      let $leader = deploy_get_value(ob1.select select /*+ READ_CONSISTENCY(WEAK) */ count(*) c from oceanbase.__all_virtual_meta_table where role = 1 and zone != 'z4' and zone != 'z3' and tenant_id = $tenant_id, c, 1);
      if ($leader > 0)
      {
        echo $primary_zone;
        echo $tenant_id;
        echo 'leader locate failed';
      }
    }
    if ($primary_zone == z1 || $primary_zone == z2)
    {
      let $leader = deploy_get_value(ob1.select select /*+ READ_CONSISTENCY(WEAK) */ count(*) c from oceanbase.__all_virtual_meta_table where role = 1 and zone != 'z1' and zone != 'z2' and tenant_id = $tenant_id, c, 1);
      if ($leader > 0)
      {
        echo $primary_zone;
        echo $tenant_id;
        echo 'leader locate failed';
      }
    }
    dec $tenant_cnt;
  }
  sleep 1;
  dec $cnt;
}
