--echo ------------------ hu_check_status_no_changed.inc ------------------

let $wait_check_time = math($proxy_info_check_interval * 2);
while($wait_check_time > 0)
{
  sleep 1;
  dec $wait_check_time;
}

let $wait_time = $wait_no_changed_time;
let $result = deploy_get_value(ob1.select_cur_tenant select hot_upgrade_cmd v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
--echo hot_upgrade_cmd      :$result;
--echo expect_parent_status :$expect_parent_status;
--echo expect_sub_status    :$expect_sub_status;
--echo wait_time            :$wait_time;

let $stop = 0;
while($stop == 0)
{
  let $parent_result = deploy_get_value(ob1.select_cur_tenant select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
  let $sub_result = deploy_get_value(ob1.select_cur_tenant select sub_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
#  --echo wait_time      :$wait_time;
#  --echo parent_result  :$parent_result;
#  --echo sub_result     :$sub_result

  if($parent_result != $expect_parent_status)
  {
    --echo $parent_result != $expect_parent_status;
    --exit
  }
  if($expect_sub_status != $sub_result)
  {
    --echo $sub_result != $expect_sub_status;
    --exit
  }

  if($stop == 0)
  {
    sleep 1;
    dec $wait_time;
    if ($wait_time == 0)
    {
      let $stop = 1;
    }
  }
}

ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy_old" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "*obproxy*" |  wc -l;

--echo ------------------ hu_check_status_no_changed.inc succ ------------------
