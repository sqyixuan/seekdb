#sys租户1号日志流切主
let $leader_port=deploy_get_value(obdocker.select select svr_port from GV\$OB_LOG_STAT where tenant_id =1 and ls_id=1 and role!="leader",svr_port,1);
let $leader_ip=deploy_get_value(obdocker.select select SVR_IP from __all_server where svr_port=$leader_port,SVR_IP,1);
obdocker.mysql alter system switch replica leader ls=1 server="$leader_ip:$leader_port" tenant=sys;
obdocker.check_until_timeout select svr_port from GV\$OB_LOG_STAT where tenant_id =1 and ls_id=1 and role="leader",$leader_port,120;

#mysql租户1号日志流切主
let $leader_port=deploy_get_value(obdocker.select select svr_port from GV\$OB_LOG_STAT where tenant_id =$mysql_tenant_id and ls_id=1 and role!="leader",svr_port,1);
let $leader_ip=deploy_get_value(obdocker.select select SVR_IP from __all_server where svr_port=$leader_port,SVR_IP,1);
obdocker.mysql alter system switch replica leader ls=1 server="$leader_ip:$leader_port" tenant=$tent1name;
obdocker.check_until_timeout select svr_port from GV\$OB_LOG_STAT where tenant_id =$mysql_tenant_id and ls_id=1 and role="leader",$leader_port,120;

#oracle租户1号日志流切主
let $leader_port=deploy_get_value(obdocker.select select svr_port from GV\$OB_LOG_STAT where tenant_id =$oracle_tenant_id and ls_id=1 and role!="leader",svr_port,1);
let $leader_ip=deploy_get_value(obdocker.select select SVR_IP from __all_server where svr_port=$leader_port,SVR_IP,1);
obdocker.mysql alter system switch replica leader ls=1 server="$leader_ip:$leader_port" tenant=$tent2name;
obdocker.check_until_timeout select svr_port from GV\$OB_LOG_STAT where tenant_id =$oracle_tenant_id and ls_id=1 and role="leader",$leader_port,120;

