ob1.mysql alter system major freeze tenant obtest;
sleep 60;
let $frozen_scn = query_get_value(select max(frozen_scn) as v from oceanbase.__all_virtual_freeze_info where tenant_id = $tenant_id, v, 1);
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $tenant_id and LAST_SCN != GLOBAL_BROADCAST_SCN and GLOBAL_BROADCAST_SCN = $frozen_scn, 0, 1200;
let $table_name = "t1";
--source include/compaction/check_major_sstable_status.inc
let $table_name = "t2";
--source include/compaction/check_major_sstable_status.inc