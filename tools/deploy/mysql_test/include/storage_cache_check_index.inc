--disable_query_log
set @@session.explicit_defaults_for_timestamp=off;
#owner : biaonian.wcx
#owner group: storage
#desc: check index status for storage cache task
# Need parameters: $table_name, $index_name, $part_name, $is_subpart
# check status
let $table_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name = '$table_name', table_id, 1);
if ($is_subpart == 0) {
  let $tablet_id = query_get_value(select tablet_id from oceanbase.__all_part where table_id = $table_id and part_name = '$part_name', tablet_id, 1);
}
if ($is_subpart == 1) {
  let $tablet_id = query_get_value(select tablet_id from oceanbase.__all_sub_part where table_id = $table_id and sub_part_name = '$part_name', tablet_id, 1);
}
let $count = 0;
while ($count == 0)
{
  let $count = query_get_value(select count(*) as count from oceanbase.__all_virtual_storage_cache_task where tablet_id = $tablet_id and status = "FINISHED" , count, 1);
  sleep 1;
}
--echo "=========== verify table partition task success ==========="
let $count = 0;
while ($count == 0)
{
  let $count = query_get_value(select count(*) as count from oceanbase.__all_virtual_tablet_local_cache where tablet_id = $tablet_id and storage_cache_policy = "HOT" , count, 1);
  sleep 1;
}
--echo "=========== verify table partition status success ==========="


let $index_table_id = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name like '%$index_name%' and data_table_id = $table_id, table_id, 1);
if ($is_subpart == 0) {
  let $tablet_id = query_get_value(select tablet_id from oceanbase.__all_part where table_id = $index_table_id and part_name = '$part_name', tablet_id, 1);
}
if ($is_subpart == 1) {
  let $tablet_id = query_get_value(select tablet_id from oceanbase.__all_sub_part where table_id = $index_table_id and sub_part_name = '$part_name', tablet_id, 1);
}
let $count = 0;
while ($count == 0)
{
  let $count = query_get_value(select count(*) as count from oceanbase.__all_virtual_storage_cache_task where tablet_id = $tablet_id and status = "FINISHED" , count, 1);
  sleep 1;
}
--echo "=========== verify index partition task success ==========="
