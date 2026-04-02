ob1.mysql alter system major freeze tenant = 'obtest';
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_MAJOR_COMPACTION where LAST_SCN != GLOBAL_BROADCAST_SCN , 1, 100;
sleep 30;