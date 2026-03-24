--echo ------------------ hu_check_drop_row_result.inc ------------------

let $parent_result = deploy_get_value(ob1.select_cur_tenant select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
let $sub_result = deploy_get_value(ob1.select_cur_tenant select sub_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);

ob1.proxy0.mysql_tenant_admin delete from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port;

let $wait_time = math($proxy_info_check_interval * 1);
while($wait_time > 0)
{
  sleep 1;
  dec $wait_time;
}

let $parent_result2 = deploy_get_value(ob1.select_cur_tenant select parent_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
let $sub_result2 = deploy_get_value(ob1.select_cur_tenant select sub_hot_upgrade_status v from test.ob_all_proxy where proxy_ip = "$ip" and proxy_port = $port, v, 1);
if($parent_result2 != $parent_result)
{
  --echo error result: $parent_result2 != $parent_result
  --exit
}
if($sub_result2 != $sub_result)
{
  --echo error result: $sub_result2 != $sub_result
  --exit
}

--echo ------------------ hu_check_drop_row_result.inc succ ------------------
