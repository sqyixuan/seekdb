connection proxy_conn;
eval ALTER RESOURCE UNIT $unit1name MAX_CPU 2, MEMORY_SIZE '4G',LOG_DISK_SIZE '10G';
obdocker.check_until_timeout select count(*) FROM oceanbase.DBA_OB_UNIT_CONFIGS WHERE NAME = "$unit1name" and MAX_CPU=2 and (MEMORY_SIZE/1073741824)=4, 1, 120;
eval ALTER RESOURCE UNIT $unit1name MAX_CPU 1, MEMORY_SIZE '3G',LOG_DISK_SIZE '10G';
obdocker.check_until_timeout select count(*) FROM oceanbase.DBA_OB_UNIT_CONFIGS WHERE NAME = "$unit1name" and MAX_CPU=1 and (MEMORY_SIZE/1073741824)=3, 1, 120;
obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL" and TENANT_ID="$oracle_tenant_id",0,1500;
--disable_all_result_log
sleep 60;
let $count=3;
while ($count > 0)
{
   let $unitnum=deploy_get_value(obdocker.select select count(*) as count from dba_ob_units where tenant_id="$oracle_tenant_id",count,1);
   if ( $unitnum == 3)
   {
        obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
        obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL",0,1500;
        obdocker.check_until_timeout select count(*) from dba_ob_units where tenant_id="$oracle_tenant_id",3,600;
        obdocker.check_until_timeout select count(*) from dba_ob_units where MIGRATE_FROM_SVR_IP IS NOT NULL,0,600;
        --error 0,4734,5156
        eval ALTER RESOURCE TENANT $tent2name UNIT_NUM = 2;
        obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
        obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL",0,1500;
        obdocker.check_until_timeout select count(*) from dba_ob_units where MIGRATE_FROM_SVR_IP IS NOT NULL,0,600;
        dec $count;
        sleep 30;
    }
   if ($unitnum == 6)
   {
        let $count = -1;
        sleep 30;
   }
}
if($count == 0)
{
   --exit
}
--enable_all_result_log
sleep 10;
obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL",0,1500;
obdocker.check_until_timeout select unit_count from DBA_OB_RESOURCE_POOLS where name="$pool2name",2,30;
obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL"  and TENANT_ID="$oracle_tenant_id",0,1500;
obdocker.check_until_timeout select count(*) from dba_ob_units where MIGRATE_FROM_SVR_IP IS NOT NULL,0,600;
obdocker.check_until_timeout select job_status from (select * from __all_rootservice_job order by 1  desc limit 1),SUCCESS,300;
eval ALTER RESOURCE TENANT $tent2name UNIT_NUM = 1;
sleep 10;
obdocker.check_until_timeout select count(*) from dba_ob_unit_jobs where job_status!="success",0,600;
obdocker.check_until_timeout select count(*) from GV\$OB_UNITS where STATUS!="NORMAL",0,1500;
obdocker.check_until_timeout select unit_count from DBA_OB_RESOURCE_POOLS where name="$pool2name",1,30;
obdocker.check_until_timeout select job_status from (select * from __all_rootservice_job order by 1  desc limit 1),SUCCESS,300;
