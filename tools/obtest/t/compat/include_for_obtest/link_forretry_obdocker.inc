let $mysql_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent1name', tenant_id, 1);
let $oracle_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent2name', tenant_id, 1);
let $notminor_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent4name', tenant_id, 1);
ob_connect (tentsql_conn,$proxyip,root@$tent1name,$tent1pw,,$proxyport);
ob_connect (tentora_conn,$proxyip,sys@$tent2name,$tent2pw,,$proxyport);
ob_connect (tentsvr_notminor,$proxyip,sys@$tent4name,$tent4pw,,$proxyport);

