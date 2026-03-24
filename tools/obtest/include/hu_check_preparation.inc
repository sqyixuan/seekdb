--echo ------------------ hu_check_preparation.inc ------------------

let $wait_time = math($proxy_info_check_interval * 1);
while($wait_time > 0)
{
  sleep 1;
  dec $wait_time;
}

let $cmd = deploy_get_value(ob1.select_cur_tenant select hot_upgrade_cmd v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
if(hot_upgrade == $cmd)
{
  --echo current cmd is hot_upgrade, can not do hot upgrader again
  --exit
}

let $version = deploy_get_value(ob1.select_cur_tenant select new_binary_version v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
if($null == $version)
{
  --echo current version is NULL, can not do hot upgrader
  --exit
}

let $md5 = deploy_get_value(ob1.select_cur_tenant select new_binary_6u_md5 v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
if($null == $md5)
{
  --echo current md5 is NULL, can not do hot upgrader
  --exit
}

--echo current hot_upgrade_cmd    :$cmd;
--echo current new_binary_version :$version;
#--echo current new_binary_6u_md5  :$md5;

#check there is only one proxy
let $pid_result = deploy_get_value(ob1.select_cur_tenant select current_pid v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
let $num = 4;
while($num > 1)
{
  let $result = deploy_get_value(ob1.select_cur_tenant show processlist, pid, 1);
  if($pid_result != $result)
  {
   --echo error, $num, $pid_result != $result;
   --exit
  }

  dec $num;
}

#update parent pid
let $parent_pid = $pid_result;
let $sub_pid = ;
#--echo current parent_pid      :$parent_pid;
#--echo current sub_pid         :$sub_pid;

$cmd_header update test.ob_all_proxy set parent_hot_upgrade_status = "" where proxy_ip = "$ip" and proxy_port = $port;
$cmd_header update test.ob_all_proxy set sub_hot_upgrade_status = "" where proxy_ip = "$ip" and proxy_port = $port;
--echo ------------------ hu_check_preparation.inc succ ------------------
