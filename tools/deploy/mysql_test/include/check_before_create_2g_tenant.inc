connect (sys,$OBMYSQL_MS0,root,,oceanbase,$OBMYSQL_PORT);

--disable_query_log
--disable_warnings
--disable_result_log
--disable_abort_on_error
let $server_count = query_get_value(select count(*) as count from __all_server, count, 1);
let $available_server_count = query_get_value(select count(*) as count from __all_virtual_server where (mem_capacity - mem_assigned) >= 2 * 1024 * 1024 * 1024, count, 1);

if ($available_server_count < $server_count)
{
  sleep 20;
}

let $available_server_count = query_get_value(select count(*) as count from __all_virtual_server where (mem_capacity - mem_assigned) >= 2 * 1024 * 1024 * 1024, count, 1);

if ($available_server_count < $server_count)
{
  --enable_query_log
  --enable_warnings
  --enable_result_log
  --enable_abort_on_error
  select * from __all_tenant;
}

--enable_query_log
--enable_warnings
--enable_result_log
--enable_abort_on_error

disconnect sys;