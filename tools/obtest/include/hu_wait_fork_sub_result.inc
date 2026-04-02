--echo ------------------ hu_wait_fork_sub_result.inc ------------------

let $random_time = query_get_value(show proxyconfig like 'fetch_proxy_bin_random_time', value, 1);
let $fetch_time = query_get_value(show proxyconfig like 'fetch_proxy_bin_timeout', value, 1);
let $expect_parent_status = create new proxy succeed;
let $expect_sub_status = new proxy created succeed;
--echo expect_parent_status :$expect_parent_status;
--echo expect_sub_status    :$expect_sub_status;

let $wait_check_time = math($proxy_info_check_interval * 2);
while($wait_check_time > 0)
{
  sleep 1;
  dec $wait_check_time;
}

connect (conn_proxy2,$ip2,admin@obproxy#MetaDataBase,admin,test,$port2);
connection conn_proxy2;

let $wait_time = math($random_time + $fetch_time + 10);
let $stop = 0;
while($stop == 0)
{
  let $parent_result = query_get_value(select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
  let $sub_result = query_get_value(select sub_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
#  --echo wait_time      :$wait_time;
#  --echo parent_result  :$parent_result;
#  --echo sub_result     :$sub_result;

  if($parent_result == $expect_parent_status)
  {
    if($sub_result != $expect_sub_status)
    {
      --echo sub result, $sub_result != $expect_sub_status;
      --exit
    }
    let $sub_pid = query_get_value(select current_pid v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
    let $stop = 1;
    --echo current parent_status:$parent_result;
    --echo current sub_status   :$sub_result;
#    --echo parent_pid           :$parent_pid;
#    --echo sub_pid              :$sub_pid;
  }
  if($stop == 0)
  {
    if($parent_result != start hot upgrade)
    {
      --echo error parent_result, $parent_result != start hot upgrade;
      --exit
    }

    sleep 1;
    dec $wait_time;
    if ($wait_time == 0)
    {
      let $stop = -1;
    }
  }
}

if($stop == -1)
{
  --echo timeout: subprocess failed to come out;
  --exit
}

ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy_old" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "*obproxy*" |  wc -l;

--echo ------------------ hu_wait_fork_sub_result.inc succ ------------------
