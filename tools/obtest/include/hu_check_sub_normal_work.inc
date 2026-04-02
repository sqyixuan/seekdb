--echo ------------------ hu_check_sub_normal_work.inc ------------------

let $result = deploy_get_value(ob1.select_cur_tenant select hot_upgrade_cmd v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
--echo hot_upgrade_cmd  :$result;
#--echo parent_pid       :$parent_pid;
#--echo sub_pid          :$sub_pid;

let $s_num = 0;
let $num = 10;
let $threshold = math($num * 0.3);

while($num > 1)
{
  let $result = deploy_get_value(ob1.select_cur_tenant show processlist, pid, 1);
#  --echo $num, $result;

  if($result == $sub_pid)
  {
#   --echo $s_num, $sub_pid == $result;
    inc $s_num;
  }
  dec $num;
}

if($s_num < $threshold)
{
--echo subproccess work unnormaly $s_num
--exit
}

--echo ------------------ hu_check_sub_normal_work.inc succ ------------------
