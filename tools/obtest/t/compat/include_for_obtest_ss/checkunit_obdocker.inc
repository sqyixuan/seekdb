--disable_all_result_log
obdocker.check_until_timeout select count(*) from DBA_OB_UNITS where ZONE="z1" and TENANT_ID="$mysql_tenant_id",1,300;
obdocker.check_until_timeout select count(*) from DBA_OB_UNITS where ZONE="z2" and TENANT_ID="$mysql_tenant_id",1,300;
obdocker.check_until_timeout select count(*) from DBA_OB_UNITS where ZONE="z1" and TENANT_ID="$oracle_tenant_id",1,300;
obdocker.check_until_timeout select count(*) from DBA_OB_UNITS where ZONE="z2" and TENANT_ID="$oracle_tenant_id",1,300;
obdocker.check_until_timeout select count(*) from DBA_OB_UNITS where ZONE="z3" and TENANT_ID="$oracle_tenant_id",1,300;
--enable_all_result_log
