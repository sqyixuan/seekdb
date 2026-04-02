connection proxy_conn;
eval alter system minor freeze tenant = $tent1name,$tent2name;
obdocker.check_until_timeout select count(*) from GV\$OB_TABLET_COMPACTION_PROGRESS,0,180;
