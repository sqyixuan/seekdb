--disable_query_log
--disable_result_log
let $max_tenant_id = query_get_value(select value from __all_sys_stat where name = 'ob_max_used_tenant_id', value, 1);

if ($max_tenant_id < 200000)
{
  update __all_sys_stat set value = 200000 where name = 'ob_max_used_tenant_id';
}
--enable_query_log
--enable_result_log