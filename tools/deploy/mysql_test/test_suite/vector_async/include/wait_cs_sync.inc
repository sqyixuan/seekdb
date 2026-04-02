## Wait for change stream to sync vector data into index_id_table/vsag.
## Caller may set $wait_cs_table (default t1) and $wait_cs_timeout (default 60).
##
## Logic:
##   Use current_scn() as target so that we wait for CDC to sync all committed
##   DML up to "now", including DELETEs (max(ORA_ROWSCN) only sees existing rows
##   and misses delete commit SCN, causing 4377 or stale results).
##   1. Brief sleep so last commit is visible, then val = current_scn()
##   2. Poll until __all_core_table.change_stream_refresh_scn >= val
--disable_query_log
--disable_result_log
if (!$wait_cs_table) {
  let $wait_cs_table = t1;
}
if (!$wait_cs_timeout) {
  let $wait_cs_timeout = 10;
}

sleep 0.2;
let $val = query_get_value(SELECT current_scn() AS max_scn, max_scn, 1);

let $__cs_wait_counter = `SELECT $wait_cs_timeout * 10`;
let $__cs_done = 0;
while ($__cs_wait_counter)
{
  let $__cs_refresh = query_get_value(SELECT column_value AS cv FROM oceanbase.__all_core_table WHERE table_name = '__all_global_stat' AND column_name = 'change_stream_refresh_scn', cv, 1);
  let $__cs_done = `SELECT $__cs_refresh >= $val`;
  if ($__cs_done)
  {
    let $__cs_wait_counter = 0;
  }
  if (!$__cs_done)
  {
    sleep 0.1;
    dec $__cs_wait_counter;
  }
}
if (!$__cs_done)
{
  --echo TIMEOUT: change_stream_refresh_scn did not reach $val within $wait_cs_timeout seconds
}
--enable_result_log
--enable_query_log
