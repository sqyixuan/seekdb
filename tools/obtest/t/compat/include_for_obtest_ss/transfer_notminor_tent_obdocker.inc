obdocker.mysql alter tenant $tent4name set primary_zone="z1,z2";
obdocker.check_until_timeout select count(*) from dba_ob_tenants where TENANT_NAME="$tent4name" and PRIMARY_ZONE like "%z1,z2;%",1,120;
sleep 30;
obdocker.check_until_timeout select count(*) from CDB_OB_TRANSFER_TASKS  where TENANT_ID = $notminor_tenant_id ,0,1200;
obdocker.check_until_timeout select STATUS from CDB_OB_TRANSFER_TASK_history where TENANT_ID = $notminor_tenant_id  order by  CREATE_TIME desc limit 1,COMPLETED,120;

obdocker.mysql alter tenant $tent4name set primary_zone="z1";
obdocker.check_until_timeout select count(*) from dba_ob_tenants where TENANT_NAME="$tent4name" and PRIMARY_ZONE like "%z1;%",1,120;
sleep 30;
obdocker.check_until_timeout select count(*) from CDB_OB_TRANSFER_TASKS  where TENANT_ID = $notminor_tenant_id,0,1200;
obdocker.check_until_timeout select STATUS from CDB_OB_TRANSFER_TASK_history where TENANT_ID = $notminor_tenant_id  order by  CREATE_TIME desc limit 1,COMPLETED,120;
