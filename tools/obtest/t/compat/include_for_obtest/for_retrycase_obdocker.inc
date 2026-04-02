let $mysql_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent1name', tenant_id, 1);
let $oracle_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent2name', tenant_id, 1);
let $notminor_tenant_id = deploy_get_value(obdocker.select select tenant_id from oceanbase.__all_tenant where tenant_name = '$tent4name', tenant_id, 1);
