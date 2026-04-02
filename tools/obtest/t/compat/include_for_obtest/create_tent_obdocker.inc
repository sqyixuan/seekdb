#创建mysql租户
connection proxy_conn;
eval CREATE RESOURCE UNIT $unit1name  MEMORY_SIZE = '3G',MAX_CPU = 1, MIN_CPU = 1,LOG_DISK_SIZE = '6G',MAX_IOPS = 10000, MIN_IOPS = 10000, IOPS_WEIGHT=1;
obdocker.check_until_timeout select count(*) FROM oceanbase.DBA_OB_UNIT_CONFIGS WHERE NAME = "$unit1name", 1, 120;
#创建资源池
eval CREATE RESOURCE POOL $pool1name UNIT= $unit1name ,UNIT_NUM=1, ZONE_LIST=('z1','z2','z3');
obdocker.check_until_timeout select count(*) FROM DBA_OB_RESOURCE_POOLS WHERE NAME = "$pool1name",1, 120;
#创建租户
eval CREATE TENANT IF NOT EXISTS $tent1name PRIMARY_ZONE='z1',locality='F@z1,F@z2,R@z3',RESOURCE_POOL_LIST=("$pool1name") ,enable_arbitration_service=true SET OB_TCP_INVITED_NODES='%' ;
obdocker.check_until_timeout select count(*) FROM DBA_OB_TENANTS WHERE TENANT_NAME = "$tent1name",1,120;
ob_connect (tentsql_conn,$proxyip,root@$tent1name,,,$proxyport);
eval alter user root identified by "$tent1pw";
let $mysql_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent1name', tenant_id, 1);
####
#创建oracle租户
connection proxy_conn;
eval CREATE RESOURCE UNIT $unit2name  MEMORY_SIZE = '3G',MAX_CPU = 1, MIN_CPU = 1,LOG_DISK_SIZE = '6G',MAX_IOPS = 10000, MIN_IOPS = 10000, IOPS_WEIGHT=1;
obdocker.check_until_timeout select count(*) FROM oceanbase.DBA_OB_UNIT_CONFIGS WHERE NAME = "$unit2name", 1, 120;
#创建资源池
eval CREATE RESOURCE POOL $pool2name UNIT= $unit2name ,UNIT_NUM=1, ZONE_LIST=('z1','z2','z3');
obdocker.check_until_timeout select count(*) FROM DBA_OB_RESOURCE_POOLS WHERE NAME = "$pool2name",1, 120;
##创建租户
eval CREATE TENANT IF NOT EXISTS $tent2name PRIMARY_ZONE='z1',RESOURCE_POOL_LIST=("$pool2name") SET OB_TCP_INVITED_NODES='%',ob_compatibility_mode='oracle';
obdocker.check_until_timeout select count(*) FROM DBA_OB_TENANTS WHERE TENANT_NAME = "$tent2name",1,120;
ob_connect (tentora_conn,$proxyip,sys@$tent2name,,,$proxyport);
eval alter user sys identified by $tent2pw;
let $oracle_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent2name', tenant_id, 1);

#创建不做转储租户
connection proxy_conn;
eval CREATE RESOURCE UNIT $unit4name  MEMORY_SIZE = '3G',MAX_CPU = 1, MIN_CPU = 1,LOG_DISK_SIZE = '6G',MAX_IOPS = 10000, MIN_IOPS = 10000, IOPS_WEIGHT=1;
obdocker.check_until_timeout select count(*) FROM oceanbase.DBA_OB_UNIT_CONFIGS WHERE NAME = "$unit4name", 1, 120;
#创建资源池
eval CREATE RESOURCE POOL $pool4name UNIT= $unit4name ,UNIT_NUM=1, ZONE_LIST=('z1','z2','z3');
obdocker.check_until_timeout select count(*) FROM DBA_OB_RESOURCE_POOLS WHERE NAME = "$pool4name",1, 120;
##创建租户
eval CREATE TENANT IF NOT EXISTS $tent4name PRIMARY_ZONE='z1',RESOURCE_POOL_LIST=("$pool4name") SET OB_TCP_INVITED_NODES='%',ob_compatibility_mode='oracle';
obdocker.check_until_timeout select count(*) FROM DBA_OB_TENANTS WHERE TENANT_NAME = "$tent4name",1,120;
ob_connect (tentnotminor_conn,$proxyip,sys@$tent4name,,,$proxyport);
eval alter user sys identified by $tent4pw;
let $notminor_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent4name', tenant_id, 1);

