connection proxy_conn;
eval alter system minor freeze tenant = $tent1name,$tent2name;
eval alter system minor freeze tenant = sys;
eval alter system minor freeze tenant = META\$$mysql_tenant_id,META\$$oracle_tenant_id;
obdocker.check_until_timeout select count(*) from GV\$OB_TABLET_COMPACTION_PROGRESS,0,180;
