#切换租户leader到新版本
--disable_all_result_log
connection proxy_conn;
let $newleadip=deploy_get_value(obdocker.z2.obs1.ip);
let $newleadport=deploy_get_value(obdocker.select select svr_port from dba_ob_units  where TENANT_ID =$mysql_tenant_id and ZONE="z2",svr_port,1);
eval alter system switch replica leader ls=1 server="$newleadip:$newleadport" tenant="$tent1name";
obdocker.check_until_timeout select count(*) from __all_virtual_log_stat where tenant_id=$mysql_tenant_id and ls_id=1 and svr_ip="$newleadip" and role="leader" and svr_port="$newleadport",1,120;
source t/compat/include_for_obtest/minor_obdocker.inc;
sleep 60;
#迁移
#新版本unit迁移
obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL",0,1500;
obdocker.check_until_timeout select count(*) from dba_ob_units where TENANT_ID=$mysql_tenant_id,3,300;
connection proxy_conn;
let $z2_unitid=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"and STATUS!="DELETING",UNIT_ID,1);
let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);
obdocker.check_until_timeout select count(*) from __all_virtual_log_stat where paxos_member_list not like "%,%",0,600;
obdocker.check_until_timeout select count(*) from dba_ob_units where MIGRATE_FROM_SVR_IP IS NOT NULL,0,600;

eval ALTER SYSTEM MIGRATE UNIT = $z2_unitid DESTINATION = '$z2_unitip:$z2_unitport';
obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$z2_unitid,$z2_unitport,60;
obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
source t/compat/include_for_obtest/minor_obdocker.inc;
sleep 60;
#老版本unit迁移
obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL",0,1500;
obdocker.check_until_timeout select count(*) from dba_ob_units where TENANT_ID=$mysql_tenant_id,3,300;
connection proxy_conn;
let $z1_unitid=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z1"and STATUS!="DELETING",UNIT_ID,1);
let $z1_unitip=deploy_get_value(obdocker.z1.obs1.ip);
let $z1_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z1" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z1"),svr_port,1);
obdocker.check_until_timeout select count(*) from __all_virtual_log_stat where paxos_member_list not like "%,%",0,600;
obdocker.check_until_timeout select count(*) from dba_ob_units where MIGRATE_FROM_SVR_IP IS NOT NULL,0,600;

eval ALTER SYSTEM MIGRATE UNIT = $z1_unitid DESTINATION = '$z1_unitip:$z1_unitport';
obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z1" and UNIT_ID=$z1_unitid,$z1_unitport,60;
obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z1" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;


#复制
connection proxy_conn;
let $newleadip=deploy_get_value(obdocker.z2.obs1.ip);
let $newleadport=deploy_get_value(obdocker.select select svr_port from dba_ob_units  where TENANT_ID =$oracle_tenant_id and ZONE="z2",svr_port,1);
eval alter system switch replica leader ls=1 server="$newleadip:$newleadport" tenant="$tent2name";
connection proxy_conn;
alter system set server_permanent_offline_time = "60s";
obdocker.check_until_timeout select distinct VALUE from  GV\$OB_PARAMETERS where name="server_permanent_offline_time",60s,10;

let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);
if ($z2_unitport==$z2_obs0port)
{
obdocker.z2.obs1.force_stop;
}
if ($z2_unitport==$z2_obs1port)
{
obdocker.z2.obs0.force_stop;
}
obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$z2_unitid,$z2_unitport,180;
obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
if ($z2_unitport==$z2_obs0port)
{
obdocker.z2.obs1.nstart;
}
if ($z2_unitport==$z2_obs1port)
{
obdocker.z2.obs0.nstart;
}
obdocker.check_until_timeout select count(*) from dba_ob_units where MIGRATE_FROM_SVR_IP IS NOT NULL,0,600;
obdocker.check_until_timeout select count(*) from oceanbase.__all_server where (start_service_time is null  or status='INACTIVE'),0,300;
source t/compat/include_for_obtest/check_schema_sync_for_restart_obdocker.inc;
alter system set server_permanent_offline_time = "3600s";
obdocker.check_until_timeout select count(*) from __all_virtual_log_stat where paxos_member_list not like "%,%",0,600;
obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
obdocker.check_until_timeout select count(*) from dba_ob_server_jobs where job_status!="success",0,600;
obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
obdocker.check_until_timeout select count(*) from dba_ob_server_jobs where job_status!="success",0,600;
source t/compat/include_for_obtest/minor_obdocker.inc;
--enable_all_result_log
