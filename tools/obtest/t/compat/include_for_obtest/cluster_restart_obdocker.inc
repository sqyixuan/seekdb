--enable_all_result_log
obdocker.z1.force_stop;
sleep 1;
obdocker.z1.start;
obdocker.check_until_timeout select count(*) from oceanbase.__all_server where (start_service_time is null  or status='INACTIVE'),0,300;
source t/compat/include_for_obtest/check_schema_sync_for_restart_obdocker.inc;
obdocker.z2.force_stop;
sleep 1;
obdocker.z2.start;
obdocker.check_until_timeout select count(*) from oceanbase.__all_server where (start_service_time is null  or status='INACTIVE'),0,300;
source t/compat/include_for_obtest/check_schema_sync_for_restart_obdocker.inc;
obdocker.z3.force_stop;
sleep 1;
obdocker.z3.start;
obdocker.check_until_timeout select count(*) from oceanbase.__all_server where (start_service_time is null  or status='INACTIVE'),0,300;
source t/compat/include_for_obtest/check_schema_sync_for_restart_obdocker.inc;
