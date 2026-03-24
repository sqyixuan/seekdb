# set __parameter_name__ outside
# set __parameter_value__ outside

--disable_query_log
--disable_result_log

connection oblog_conn_sys;

let $__tenant_id__ = query_get_value(select tenant_id from oceanbase.__all_tenant where tenant_name = '$mysql_tenant_name', tenant_id, 1);
let $__i__= 60;
while($__i__ > 0)
{
  sleep 1;
  dec $__i__;
  let $__value__= query_get_value(select value from oceanbase.__all_virtual_tenant_parameter_stat where name = '$__parameter_name__' and (scope = 'CLUSTER' or (scope = 'TENANT' and tenant_id = $__tenant_id__)), value, 1);
  if ($__value__ == $__parameter_value__)
  {
    let $__i__ = -5;
  }
}

--enable_query_log
--enable_result_log
