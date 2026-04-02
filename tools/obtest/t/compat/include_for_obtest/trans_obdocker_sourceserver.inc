#迁移
connection proxy_conn;
let $z2_unitid=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"and STATUS!="DELETING",UNIT_ID,1);
let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);

eval ALTER SYSTEM MIGRATE UNIT = $z2_unitid DESTINATION = '$z2_unitip:$z2_unitport';
obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$z2_unitid,$z2_unitport,60;
obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
#
connection proxy_conn;
##1、设置同region,不同IDC的部署模式
alter system alter zone z1 set region 'shanghai', IDC 'idc1';
alter system alter zone z2 set region 'shanghai', IDC 'idc2';
alter system alter zone z3 set region 'shanghai', IDC 'idc3';
let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);
let $unit_id=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"and STATUS!="DELETING",UNIT_ID,1);
let $src_ls_id = deploy_get_value(obdocker.select select ls_id from __all_virtual_ls_meta_table where unit_id = $unit_id and tenant_id = $mysql_tenant_id, ls_id, 1);
if ($z2_unitport==$z2_obs0port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs0port, 1, 100;
##迁移时优先选择副本                                        
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs1port";
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs1port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}
if ($z2_unitport==$z2_obs1port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs1port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs0port";
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs0port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}

#一个idc有主有副
alter system alter zone z1 set region 'shanghai', IDC 'idc1';
alter system alter zone z2 set region 'shanghai', IDC 'idc1';
alter system alter zone z3 set region 'shanghai', IDC 'idc1';
let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);
let $unit_id=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"and STATUS!="DELETING",UNIT_ID,1);
let $src_ls_id = deploy_get_value(obdocker.select select ls_id from __all_virtual_ls_meta_table where unit_id = $unit_id and tenant_id = $mysql_tenant_id, ls_id, 1);
if ($z2_unitport==$z2_obs0port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs0port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs1port";
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs1port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}
if ($z2_unitport==$z2_obs1port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs1port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs0port";
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs0port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}

#同一regin仅主或者仅副
alter system set choose_migration_source_policy = 'region' SCOPE = BOTH TENANT = all_user;
alter system alter zone z1 set region 'shanghai1', IDC 'idc1';
alter system alter zone z2 set region 'shanghai2', IDC 'idc1';
alter system alter zone z3 set region 'shanghai3', IDC 'idc1';
let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);
let $unit_id=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"and STATUS!="DELETING",UNIT_ID,1);
let $src_ls_id = deploy_get_value(obdocker.select select ls_id from __all_virtual_ls_meta_table where unit_id = $unit_id and tenant_id = $mysql_tenant_id, ls_id, 1);
if ($z2_unitport==$z2_obs0port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs0port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs1port";
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs1port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}
if ($z2_unitport==$z2_obs1port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs1port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs0port";
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs0port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}

#region模式
alter system set choose_migration_source_policy = 'region' SCOPE = BOTH TENANT = all_user;
alter system alter zone z1 set region 'shanghai1', IDC 'idc1';
alter system alter zone z2 set region 'shanghai2', IDC 'idc1';
alter system alter zone z3 set region 'shanghai1', IDC 'idc2';
let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);
let $unit_id=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"and STATUS!="DELETING",UNIT_ID,1);
let $src_ls_id = deploy_get_value(obdocker.select select ls_id from __all_virtual_ls_meta_table where unit_id = $unit_id and tenant_id = $mysql_tenant_id, ls_id, 1);
if ($z2_unitport==$z2_obs0port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs0port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs1port";
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs1port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}
if ($z2_unitport==$z2_obs1port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs1port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs0port";
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs0port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
}


#不开源端选择
alter system set _enable_choose_migration_source_policy = false SCOPE = BOTH TENANT = all_user;
let $z2_unitip=deploy_get_value(obdocker.z2.obs1.ip);
let $z2_unitport=deploy_get_value(obdocker.select select svr_port from __all_server where zone="z2" and svr_port!=(select svr_port from  DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"),svr_port,1);
let $unit_id=deploy_get_value(obdocker.select select UNIT_ID from DBA_OB_UNITS where TENANT_ID = $mysql_tenant_id and ZONE="z2"and STATUS!="DELETING",UNIT_ID,1);
let $src_ls_id = deploy_get_value(obdocker.select select ls_id from __all_virtual_ls_meta_table where unit_id = $unit_id and tenant_id = $mysql_tenant_id, ls_id, 1);
if ($z2_unitport==$z2_obs0port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs0port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs1port";
  obdocker.check_until_timeout select count(1) from oceanbase.__all_server_event_history where event like 'choose_src' and value3 like "%$z2_unitip:$z2_unitport%", 3, 100;
  obdocker.mysql delete from oceanbase.__all_server_event_history where event like 'choose_src';
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs1port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  ob1.check_until_timeout select count(1) > 0 from oceanbase.__all_server_event_history where event like 'choose_src', 1, 100;
}
if ($z2_unitport==$z2_obs1port)
{
  obdocker.mysql alter system switch replica leader ls=$src_ls_id server='$z2_unitip:$z2_unitport' tenant='$tent2name';
  obdocker.check_until_timeout select role from oceanbase.__all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $src_ls_id and svr_ip = '$z2_unitip:$z2_unitport' and svr_port = $z2_obs1port, 1, 100;
##迁移时优先选择副本
  obdocker.mysql alter system migrate unit $unit_id destination "$z2_unitip:$z2_obs0port";
  obdocker.check_until_timeout select SVR_PORT from DBA_OB_UNITS where zone="z2" and UNIT_ID=$unit_id,$z2_obs0port,60;
  obdocker.check_until_timeout select count(*) from  DBA_OB_UNITS where TENANT_ID =$mysql_tenant_id and ZONE="z2" and MIGRATE_FROM_SVR_IP IS NULL and MIGRATE_FROM_SVR_PORT IS NULL and MANUAL_MIGRATE IS NULL,1,120;
  ob1.check_until_timeout select count(1) > 0 from oceanbase.__all_server_event_history where event like 'choose_src', 1, 100;
}
