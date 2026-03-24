# input: $table_name, $partition_name, $mode (MERGE/LATEST, get the last merge scn or the latest scn)
# output: types of major sstables of $table_name
# notice: only 1 tablet in $table_name, no partition, connection is managed outside

--disable_query_log
if ($mode == 0) 
{
  let $selected_scn = query_get_value(select last_scn from oceanbase.DBA_OB_MAJOR_COMPACTION, last_scn, 1);
} 
if ( $mode == 1) 
{
  eval SET @max_scn = (select max(end_log_scn) as max_scn from oceanbase.v\$ob_sstables where tablet_id = (select tablet_id from oceanbase.DBA_OB_TABLE_LOCATIONS where table_name = $table_name and partition_name = $partition_name and role = 'leader') and (table_type = 'MAJOR' or table_type = 'CO_MAJOR'));
  let $selected_scn = query_get_value(select @max_scn as max_scn, max_scn, 1);
}
eval select count(1) from oceanbase.v\$ob_sstables where tablet_id = (select tablet_id from oceanbase.DBA_OB_TABLE_LOCATIONS where table_name = $table_name and partition_name = $partition_name and role = 'leader') and end_log_scn = $selected_scn and table_type in ('MAJOR', 'CO_MAJOR', 'NORMAL_CG', 'ROWKEY_CG');
eval select table_type from oceanbase.v\$ob_sstables where tablet_id = (select tablet_id from oceanbase.DBA_OB_TABLE_LOCATIONS where table_name = $table_name and partition_name = $partition_name and role = 'leader') and end_log_scn = $selected_scn and table_type in ('MAJOR', 'CO_MAJOR', 'NORMAL_CG', 'ROWKEY_CG');
--enable_query_log
