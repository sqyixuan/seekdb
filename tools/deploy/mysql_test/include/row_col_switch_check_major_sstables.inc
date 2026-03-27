# make sure connection obsys to tenant root@sys exists
--echo [INFO] check cg status for $table_name

connection obsys;

let $last_scn = query_get_value(select LAST_SCN from CDB_OB_MAJOR_COMPACTION, LAST_SCN, 1);
let $tablet_id = query_get_value(select tablet_id from __all_virtual_table where table_name = $table_name, tablet_id, 1);

--disable_query_log
--echo [INFO] get the major sstable number of $table_name
eval select /*+QUERY_TIMEOUT(100000000)*/ count(*) as major_cnt from V\$OB_SSTABLES where TABLET_ID = $tablet_id and END_LOG_SCN = $last_scn and TABLE_TYPE in ('MAJOR', 'CO_MAJOR', 'NORMAL_CG', 'ROWKEY_CG');
--echo [INFO] get the major sstable types of $table_name
eval select /*+QUERY_TIMEOUT(100000000)*/ TABLE_TYPE from V\$OB_SSTABLES WHERE TABLET_ID = $tablet_id and END_LOG_SCN = $last_scn and TABLE_TYPE in ('MAJOR', 'CO_MAJOR', 'NORMAL_CG', 'ROWKEY_CG');
--echo [INFO] get the count of non-zero data checksum  of $table_name
eval select /*+QUERY_TIMEOUT(100000000)*/ count(*) > 0 as non_zero_ckm from __all_virtual_tablet_replica_checksum where tablet_id = $tablet_id and data_checksum > 0;
--enable_query_log

connection default;
