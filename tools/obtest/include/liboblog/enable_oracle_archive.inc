# enable archivelog for oracle tenant;
# use $oracle_tenant_name to specify the tenant which user want to enable archivelog on;
# use $original_conn to switch to the connection which user used before sourcing this script;

ob_connect(conn_sys_tenant_oracle_archive, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
let $backup_tenant_id = query_get_value(select tenant_id from oceanbase.__all_tenant where tenant_name='$oracle_tenant_name', tenant_id, 1);
ob_connect(conn_oracle_tenant_oracle_archive, ob1.z1.obs0, SYS@$oracle_tenant_name, ,SYS, , USE_OB_CONNECTOR);
let $log_backup_dest = deploy_get_value(ob1.get_log_backup_dest oracle);
--disable_all_result_log
eval alter system set log_archive_dest='location=$log_backup_dest';
--enable_all_result_log
ob1.check_until_timeout select count(*) as num from oceanbase.__all_virtual_archive_stat, 0, 60;
ob1.mysql alter system archivelog;
ob1.check_until_timeout select status from oceanbase.CDB_OB_ARCHIVELOG where tenant_id=$backup_tenant_id, DOING, 600;
source include/liboblog/archive_lag_target.inc;

connection $original_conn;