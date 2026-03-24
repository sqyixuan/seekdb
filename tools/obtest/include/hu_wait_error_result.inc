--echo ------------------ hu_wait_error_result.inc ------------------

let $wait_time = $wait_error_result_time;
--echo expect_parent_status :$expect_parent_status;
--echo wait_time            :$wait_time;

let $stop = 0;
let $wait_time = math($wait_time * 2);
while($wait_time > 0)
{
  let $parent_result = deploy_get_value(ob1.select_cur_tenant select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
  if($parent_result == $expect_parent_status)
  {
    let $stop = 1;
    let $wait_time = 0;
    --echo parent_result        :$parent_result;
  }
  if ($stop != 1)
  {
    sleep 0.5;
    dec $wait_time;
  }
}

if($stop != 1)
{
  --echo timeout, error result: $parent_result;
  --exit
}


ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy_old" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "*obproxy*" |  wc -l;

--echo ------------------ hu_wait_error_result.inc succ ------------------
