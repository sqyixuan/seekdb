--disable_all_result_log
obdocker.check_until_timeout select count(*) from CDB_OB_MAJOR_COMPACTION where `GLOBAL_BROADCAST_SCN`!=`LAST_SCN` or STATUS!="IDLE",0,2000;
--enable_all_result_log
ALTER SYSTEM MAJOR FREEZE TENANT =all;
obdocker.check_until_timeout select count(*) from CDB_OB_MAJOR_COMPACTION where `GLOBAL_BROADCAST_SCN`!=`LAST_SCN` or STATUS!="IDLE",0,2000;
