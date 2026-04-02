#1.6仲裁升降级
let $degrade_ip=deploy_get_value(obdocker.select select distinct svr_ip from GV\$OB_LOG_STAT where tenant_id=$mysql_tenant_id and SVR_IP in (select svr_ip from __all_server where zone="z2") limit 1,svr_ip,1);

obdocker.z2.force_stop;
obdocker.check_until_timeout  select count(*) from __all_virtual_log_stat where tenant_id=$mysql_tenant_id and degraded_list not like "$degrade_ip%",0,180;

obdocker.z2.nstart;
obdocker.check_until_timeout  select count(*) from __all_virtual_log_stat where tenant_id=$mysql_tenant_id and degraded_list not like "",0,180;
obdocker.check_until_timeout select count(*) from oceanbase.__all_server where (start_service_time is null  or status='INACTIVE'),0,300;
source t/compat/include_for_obtest_ss/check_schema_sync_for_restart_obdocker.inc;
