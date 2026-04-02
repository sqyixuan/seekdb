let $rootservice_port=deploy_get_value(obdocker.select select svr_port from DBA_OB_SERVERS where WITH_ROOTSERVER="YES",svr_port,1);
let $rootservice_zone=deploy_get_value(obdocker.select select distinct zone from __all_server where zone not in ( select zone from __all_server where with_rootserver=1),zone,1);
obdocker.mysql alter tenant sys set primary_zone = "$rootservice_zone";
obdocker.check_until_timeout  select zone from DBA_OB_LS_LOCATIONS where role="leader",$rootservice_zone,120;
obdocker.check_until_timeout select count(*) from DBA_OB_SERVERS where WITH_ROOTSERVER="YES" and svr_ip = "$rootservice_port",0,120;
obdocker.check_until_timeout select count(*) from oceanbase.__all_server where (start_service_time is null  or status='INACTIVE'),0,300;
source t/compat/include_for_obtest/check_schema_sync_for_restart_obdocker.inc;
