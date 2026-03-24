let $__i__ = 60;
let $__is_scp_prewarm_task_exist__ = 0;
let $__need_check_scp_prewarm_task_speed__ = 0;
if ($expect_scp_prewarm_task_exist)
{
  let $__need_check_scp_prewarm_task_speed__ = 1;
}

while($__i__ > 0)
{
  dec $__i__;
  sleep 1;
  let $__task_count__ = query_get_value(select count(*) as task_count from oceanbase.__all_virtual_storage_cache_task where tenant_id = $check_scp_task_tenant_id and tablet_id = $check_scp_task_tablet_id and status = "FINISHED" and start_time > "$check_scp_task_init_timestamp", task_count, 1);
  if ($__task_count__ > $server_count)
  {
    --echo doing_task_count: $__task_count__ is unexpected, expected_count: $server_count
  }
  if ($__task_count__ == $server_count)
  {
    if ($__need_check_scp_prewarm_task_speed__)
    {
      let $__speed_valid__ = `select cast(regexp_substr(speed, '[-]?[0-9]+\.?[0-9]*') as float) > 0 from oceanbase.__all_virtual_storage_cache_task where tenant_id = $check_scp_task_tenant_id and tablet_id = $check_scp_task_tablet_id and status = "FINISHED" and start_time > "$check_scp_task_init_timestamp"`;
      if ($__speed_valid__)
      {
        let $__i__ = -5;
      }
    }

    if (! $__need_check_scp_prewarm_task_speed__)
    {
      # no need to check speed
      let $__i__ = -5;
    }
  }
}

if ($__i__ == -5)
{
  let $__is_scp_prewarm_task_exist__ = 1;
}
if ($expect_scp_prewarm_task_exist != $__is_scp_prewarm_task_exist__)
{
  if ($expect_scp_prewarm_task_exist)
  {
    --echo fail to get storage cache task status
  }

  if (! $expect_scp_prewarm_task_exist)
  {
    --echo get storage cache task status unexpected
  }

  --echo $check_scp_task_init_timestamp, $check_scp_task_tenant_id, $check_scp_task_tablet_id
}