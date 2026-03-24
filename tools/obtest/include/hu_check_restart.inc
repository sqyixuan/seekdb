--echo ------------------ hu_check_restart.inc ------------------

let $expect_parent_status = create new proxy succeed;
let $expect_sub_status = new proxy created succeed;
#--echo expect_parent_status :$expect_parent_status;
#--echo expect_sub_status    :$expect_sub_status;
#--echo parent_pid           :$parent_pid;

connect (conn_proxy2,$ip2,admin@obproxy#MetaDataBase,admin,test,$port2);
connection conn_proxy2;
# query_get_value will use conn2
# others will use default

sleep 1;

let $wait_time = 5;
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
#    --echo parent_pid           :$parent_pid;
#    --echo sub_pid              :$sub_pid;
    --echo current parent_status:$parent_result;
    --echo current sub_status   :$sub_result;
  }
  if($stop == 0)
  {
    if($parent_result != start doing restart)
    {
      --echo error parent_result, $parent_result != start doing restart;
      --exit
    }

    sleep 0.5;
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

let $wait_commit_time = math($proxy_info_check_interval * 4);
while ($wait_commit_time > 0)
{
   sleep 1;
   dec $wait_commit_time;
}

--disable_query_log
eval select hot_upgrade_cmd, parent_hot_upgrade_status, sub_hot_upgrade_status, new_binary_version from test.ob_all_proxy where proxy_ip ="$ip" and proxy_port=$port;
--enable_query_log

#make the blocking accept accept connection
let $accpet_thread_count = 2;
let $use_blocking_accpet = math($accpet_thread_count * 3);
while($use_blocking_accpet > 0)
{
  --error 0, 255
  connect (conn_inner,$ip,admin@obproxy#MetaDataBase,admin,test,$port);
  connection conn_inner;
  dec $use_blocking_accpet;
}

let $exit_timeout = query_get_value(show proxyconfig like 'hot_upgrade_graceful_exit_timeout', value, 1);
let $wait_time = math($exit_timeout + $proxy_info_check_interval);
while($wait_time > 0)
{
  sleep 1;
  dec $wait_time;
}

sleep 1;

ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "*obproxy*" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "*obproxy*";

ob1.proxy0.sh_p ps xf | cat | grep "$parent_pid" -c;
ob1.proxy0.sh_p ps xf | cat | grep "$sub_pid" -c;

--disable_query_log
eval select hot_upgrade_cmd, parent_hot_upgrade_status, sub_hot_upgrade_status, new_binary_version from test.ob_all_proxy where proxy_ip ="$ip" and proxy_port=$port;
--enable_query_log

let $parent_pid = $sub_pid;
--echo ------------------ hu_check_restart.inc succ ------------------
