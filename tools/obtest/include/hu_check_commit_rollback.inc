--echo ------------------ hu_check_commit_rollback.inc ------------------

let $wait_time = 10;
let $obproxy_count = deploy_get_value(ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy" |  wc -l);
while ($wait_time > 0)
{
  if ($obproxy_count != $expect_obproxy_count)
  {
    sleep 1;
    dec $wait_time;
    let $obproxy_count = deploy_get_value(ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy" |  wc -l);
  }
  if ($obproxy_count == $expect_obproxy_count)
  {
    let $wait_time = -1;
  }
}

if ($wait_time == 0)
{
  --echo unexpected: $obproxy_count != $expect_obproxy_count;
  --exit
}

connect (conn_proxy2,$ip2,admin@obproxy#MetaDataBase,admin,test,$port2);
connection conn_proxy2;
# query_get_value will use conn2
# others will use default

let $wait_time = math($proxy_info_check_interval * 2);
while($wait_time > 0)
{
  sleep 1;
  dec $wait_time;
}

let $result = query_get_value(select hot_upgrade_cmd v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
--echo current hot_upgrade_cmd  :$result;
--echo current input_cmd        :$input_cmd;
#--echo current parent_pid       :$parent_pid;
#--echo current sub_sub_pid      :$sub_pid;


let $timestamp1 = query_get_value(select unix_timestamp(), unix_timestamp(), 1);
let $stop = 0;
if($input_cmd == commit)
{
  let $status = query_get_value(select sub_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
  --echo sub_hot_upgrade_status:$status;
  if($status != commit succeed)
  {
    let $stop = -1;
    --echo sub_hot_upgrade_status is not 'commit succeed', it is $status;
    --exit
  }
  if($status == commit succeed)
  {
    let $parent_status = query_get_value(select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
    if ($parent_status != received commit, do graceful exit)
    {
      let $stop = -1;
      --echo parent_hot_upgrade_status is not 'received commit, do graceful exit', it is $parent_status;
      --exit
    }
    let $stop = 1;
    let $pid = $sub_pid;
    let $npid = $parent_pid;
  }
}

if($input_cmd == rollback)
{
  let $status = query_get_value(select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
  --echo parent_hot_upgrade_status:$status;
  let $expect_status = rollback succeed;
  let $expect_sub_status = received rollback, do graceful exit;
  if($timeout_rollback == 1)
  {
    let $expect_status = timeout rollback succeed;
    let $expect_sub_status = received timeout rollback, do graceful exit;
  }
  if($status != $expect_status)
  {
    let $stop = -1;
    --echo parent_hot_upgrade_status is not $expect_status, it is $status;
    --exit
  }
  if($status == $expect_status)
  {
    let $sub_status = query_get_value(select sub_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
    if ($sub_status != $expect_sub_status)
    {
      let $stop = -1;
      --echo sub_hot_upgrade_status is not $expect_sub_status, it is $sub_status;
      --exit
    }
    let $stop = 1;
    let $pid = $parent_pid;
    let $npid = $sub_pid;
  }
}

if($stop != 1)
{
  --echo it should not come here, stop:$stop;
  --exit
}

let $pid_result = query_get_value(select current_pid v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
if($npid == $pid_result)
{
  --echo current_pid is not update, $pid_result
  --exit
}
if($pid != $pid_result)
{
  --echo current_pid is not update, $pid_result
  --exit
}

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

#check there is only one proxy leave
let $num = 4;
while($num > 1)
{
  let $pid_result = deploy_get_value(ob1.select_cur_tenant show processlist, pid, 1);
#  --echo $num, $pid_result;

  if($pid_result != $pid)
  {
   --echo error, $number, $pid_result != $pid;
   --exit
  }
  if($pid_result == $npid)
  {
   --echo error, $num, $pid_result == $npid;
   --exit
  }

  dec $num;
}
let $parent_pid = $sub_pid;

#check exit status
let $timestamp2 = deploy_get_value(ob1.select_cur_tenant select unix_timestamp(), unix_timestamp(), 1);
let $exit_timeout = query_get_value(show proxyconfig like 'hot_upgrade_graceful_exit_timeout', value, 1);
let $wait_time = math($exit_timeout + $timestamp1 + $proxy_info_check_interval - $timestamp2);
while($wait_time > 0)
{
  sleep 1;
  dec $wait_time;
}

sleep 1;

ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "obproxy_old" |  wc -l;
ob1.proxy0.sh_p find ~/ob1.proxy0/bin -name "*obproxy*" |  wc -l;

#ob1.proxy0.sh_p ps xf;
ob1.proxy0.sh_p ps xf | cat | grep "$npid" -c;
ob1.proxy0.sh_p ps xf | cat | grep "$pid" -c;

let $result = deploy_get_value(ob1.select_cur_tenant select hot_upgrade_cmd v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
--echo hot_upgrade_cmd  :$result;
let $result = deploy_get_value(ob1.select_cur_tenant select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
--echo parent_result    :$result;
let $result = deploy_get_value(ob1.select_cur_tenant select sub_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
--echo sub_result       :$result;


--echo ------------------ hu_check_commit_rollback.inc succ ------------------
