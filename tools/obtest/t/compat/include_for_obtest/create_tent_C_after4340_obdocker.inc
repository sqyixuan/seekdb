
#创建不做转储租户
connection proxy_conn;
##创建租户
eval drop tenant $tent4name;
sleep 10;
eval CREATE TENANT IF NOT EXISTS $tent4name PRIMARY_ZONE='z1',locality='F@z1,F@z2,C@z3',RESOURCE_POOL_LIST=("$pool4name"),enable_arbitration_service=true SET OB_TCP_INVITED_NODES='%',ob_compatibility_mode='oracle';
obdocker.check_until_timeout select count(*) FROM DBA_OB_TENANTS WHERE TENANT_NAME = "$tent4name",1,120;
ob_connect (tentnotminor_conn,$proxyip,sys@$tent4name,,,$proxyport);
eval alter user sys identified by $tent4pw;
let $notminor_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent4name', tenant_id, 1);

