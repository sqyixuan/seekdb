
--disable_query_log
set @@session.explicit_defaults_for_timestamp=off;
#owner : biaonian.wcx
#owner group: storage
#desc: common check for storage cache task
# Enhanced verification script for all partitions and subpartitions
# Required parameters: $table_name, $target_verified_partitions




# Get table_id
let $table_id = query_get_value(SELECT table_id FROM oceanbase.__all_virtual_table WHERE table_name = '$table_name', table_id, 1);
if (!$table_id) {
  --echo ERROR: Table $table_name not found
  exit;
}

connection ob_mysql;
# Initialize counters
let $total_partitions = 0;
let $verified_partitions = 0;

--disable_warnings
DROP TABLE IF EXISTS tmp_partitions;
DROP TABLE IF EXISTS tmp_verified_partitions;
--enable_warnings

# Get main partitions
CREATE TABLE tmp_partitions (
  id INT AUTO_INCREMENT PRIMARY KEY,
  tablet_id BIGINT,
  table_id BIGINT,
  part_name VARCHAR(64)
);
CREATE TABLE tmp_verified_partitions (
  tablet_id BIGINT,
  part_name VARCHAR(64),
  UNIQUE KEY (tablet_id)
);

# Insert main partitions
eval INSERT INTO tmp_partitions (tablet_id, table_id, part_name) SELECT tablet_id, table_id, part_name FROM oceanbase.__all_part WHERE table_id = $table_id;
# Insert subpartitions
eval INSERT INTO tmp_partitions (tablet_id, table_id, part_name) SELECT tablet_id, table_id, sub_part_name FROM oceanbase.__all_sub_part WHERE table_id = $table_id;

# Get total count
let $total_partitions = query_get_value(SELECT COUNT(*) as count FROM tmp_partitions, count, 1);

# Verification loop
let $remain_time = 300;
while ($remain_time > 0) {
  # Get partition name
  let $verified_partitions = 0;
  let $current_id = 1;
  let $result_partition = "";
  while ($current_id <= $total_partitions) {
    # Get partition info
    let $tablet_id = query_get_value(SELECT tablet_id FROM tmp_partitions where table_id = $table_id and id = $current_id, tablet_id, 1);

    # Verify task exists
    if ($tablet_id != 0) {
      let $task_exist = query_get_value(SELECT COUNT(*) as count FROM oceanbase.__all_virtual_storage_cache_task WHERE tablet_id = $tablet_id, count, 1);
      if ($task_exist == $server_count) {
        inc $verified_partitions;
        let $part_name = query_get_value(SELECT part_name FROM tmp_partitions WHERE tablet_id = $tablet_id, part_name, 1);
        eval INSERT IGNORE INTO tmp_verified_partitions (tablet_id, part_name) VALUES ($tablet_id, '$part_name');
      }
    }
    sleep 1;
    dec $remain_time;
    inc $current_id;
  }
  if ($verified_partitions == $target_verified_partitions) {
    --echo All partitions verified for table $table_name
    let $remain_time = -5;
  }
  if ($remain_time > 0) {
    sleep 1;
    dec $remain_time;
  }
}

if ($remain_time != -5) {
  let $verified_partitions = query_get_value(SELECT * FROM tmp_verified_partitions, verified_partitions, 1);
  --echo ERROR: Not all target partitions verified,target_verified_partitions=$target_verified_partitions, verified_partitions=$verified_partitions, total_partitions=$total_partitions, remain_time=$remain_time, table_name=$table_name, table_id=$table_id
  --echo $verified_partitions
  exit;
}

--disable_warnings
# Cleanup
DROP TABLE IF EXISTS tmp_partitions;
# tmp_verified_partitions will be used in the next step, so do not drop it here