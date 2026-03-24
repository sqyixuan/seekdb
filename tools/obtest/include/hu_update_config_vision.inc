--echo -- hu_update_config_vision.inc

let $version = deploy_get_value(ob1.select_cur_tenant select value v from test.ob_all_proxy_kv_table where name='ob_proxy_config_version', v, 1);
let $version = math($version + 1);
#--echo $vision;

eval update test.ob_all_proxy_kv_table set value = '$version' where name = 'ob_proxy_config_version';
eval update test.ob_all_proxy set reload_config = 'reloading' where proxy_port = $port and proxy_ip = "$ip";

sleep 3;
let $wait_time = 5;
while($wait_time > 0)
{
  let $result = deploy_get_value(ob1.select_cur_tenant select config_version v from test.ob_all_proxy where proxy_port = $port and proxy_ip = "$ip", v, 1);
#  --echo $wait_time $result:$version

  if($result == $version)
  {
    let $result = deploy_get_value(ob1.select_cur_tenant select reload_config v from test.ob_all_proxy where proxy_port = $port and proxy_ip = "$ip", v, 1);
    if($result != reload success)
    {
      --echo result error, reload_config:$result
      --exit
    }

    let $wait_time = 0;
  }
  sleep 1;
  dec $wait_time;
}

if($wait_time == 0)
{
--echo timeout, failed to set config
--exit
}

#wait config server update
let $wait_time = query_get_value(show proxyconfig like 'config_server_refresh_interval', value, 1);
while($wait_time > 0)
{
  let $result = query_get_value(show proxyconfig like 'json_config_version', value, 1);
  if($result == $json_config_version)
  {
    let $wait_time = 0;
  }
  sleep 1;
  dec $wait_time;
}

if($wait_time == 0)
{
--echo timeout, failed to update config server
--exit
}

--echo -- hu_update_config_vision.inc succ
