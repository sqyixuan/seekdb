#PREREQUISITE: 1. include this file under sys tenant's connection
#              2. define <$__tenant_name__> and <$__archive_lag_target__> in advance
#              3. include this file after setting log_archive_dest
--disable_all_result_log
eval alter system set archive_lag_target='60s' tenant = '$__tenant_name__';
eval alter system set log_archive_concurrency = 1 tenant = '$__tenant_name__';
sleep 2;
let $set_archive_lag_tenant_id = query_get_value(select tenant_id from oceanbase.__all_tenant where tenant_name = '$__tenant_name__', tenant_id, 1);
let $set_archive_lag_meta_tenant_id = $set_archive_lag_tenant_id;
dec $set_archive_lag_meta_tenant_id;
eval alter system change tenant tenant_id = $set_archive_lag_meta_tenant_id;
eval update oceanbase.__tenant_parameter set value='$__archive_lag_target__', config_version=time_to_usec(now()) where name='archive_lag_target';
sleep 2;
alter system change tenant sys;
eval alter system set log_archive_concurrency = 0 tenant = '$__tenant_name__';
--enable_all_result_log