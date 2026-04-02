source include/mini_ob_config.inc;
ob1.z1.obs0.set extra_config = "cache_wash_threshold='1G',_ob_minor_merge_schedule_interval='3s'";

let $backup_cluster_name = deploy_get_value(ob1.get_cluster_name);
let $backup_cluster_id = deploy_get_value(ob1.get_cluster_id);
let $backup_ip = deploy_get_value(ob1.z1.obs0.ip);
let $backup_port = deploy_get_value(ob1.z1.obs0.mysql_port);
let $restore_ip = deploy_get_value(ob1.z1.obs0.ip);
let $restore_port = deploy_get_value(ob1.z1.obs0.mysql_port);
let $backup_dest = deploy_get_value(ob1.get_backup_dest);
let $backup_mysql_tenant = backup_mysql_tenant;
let $restore_mysql_tenant = restore_mysql_tenant;
let $log_backup_dest = deploy_get_value(ob1.get_log_backup_dest $backup_mysql_tenant);
let $data_backup_dest = deploy_get_value(ob1.get_data_backup_dest $backup_mysql_tenant);
let $restore_log_archive_dest = deploy_get_value(ob1.get_log_backup_dest $restore_mysql_tenant);

# 必须先stop掉老的，删除才能有效，不然可能会有残留。
ob1.force_stop;
ob1.clean_backup_dest;
ob1.reboot;

# 固定sys租户规格
ob1.mysql alter resource unit sys_unit_config memory_size='2G';

connect(backup_sys_conn, $backup_ip, root@sys, , oceanbase, $backup_port);
connection backup_sys_conn;
use oceanbase;
if ($multi_zone_flag == 0)
{
  source sql/physical_backup/prepare_single_zone_mini_resource.sql;
}
if ($multi_zone_flag == 1)
{
  source sql/physical_backup/prepare_multi_zone_resource.sql;
}

ob1.mysql alter system set _backup_idle_time = '10s';
ob1.mysql alter system set merger_check_interval = '10s';
ob1.mysql alter system set enable_async_syslog=false;

# create mysql tenant, test create tenant before major freeze
ob1.mysql CREATE TENANT $backup_mysql_tenant RESOURCE_POOL_LIST = ('small_pool'), primary_zone = "z1" set ob_tcp_invited_nodes='%', ob_compatibility_mode='mysql';
let $backup_mysql_tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name='$backup_mysql_tenant', tenant_id, 1);

# mysql租户  创建tablet
ob_connect(sys_conn_mysql, ob1.z1.obs0, root@$backup_mysql_tenant, , oceanbase, , USE_MYSQL_CONNECTOR);
connection sys_conn_mysql;
set global wait_timeout = 288000;

--disable_all_result_log
eval alter system set data_backup_dest='$data_backup_dest';
eval alter system set log_archive_dest='location=$log_backup_dest piece_switch_interval=1d';
--enable_all_result_log
ob_connect(backup_mysql_conn, ob1.z1.obs0, root@$backup_mysql_tenant, , test, , USE_MYSQL_CONNECTOR);
connection backup_mysql_conn;

create table t1(a int, b int, c int);
insert into t1(a, b) values(1, 1);

# mysql租户  开启日志备份
ob1.mysql alter system archivelog;
ob1.check_until_timeout select status from oceanbase.CDB_OB_ARCHIVELOG where tenant_id=$backup_mysql_tenant_id, DOING, 600;

# Note: there may be unfinished major_merge, which are launched according to major_freeze_duty_time.
# Therefore, we need to disable daily_merge and wait the launched major_merge to finish here.
ob1.mysql alter system set major_freeze_duty_time = 'Disable' tenant sys;
ob1.mysql alter system set major_freeze_duty_time = 'Disable' tenant all_user;
ob1.mysql alter system set major_freeze_duty_time = 'Disable' tenant all_meta;
ob1.check_until_timeout select frozen_scn=last_scn from CDB_OB_MAJOR_COMPACTION where tenant_id = $backup_mysql_tenant_id, 1, 600;

ob1.mysql alter system major freeze tenant = '$backup_mysql_tenant';
let $max_frozen_scn = `select max(frozen_scn) from oceanbase.__all_freeze_info`;
ob1.check_until_timeout select (frozen_scn - last_scn) as result from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $backup_mysql_tenant_id and frozen_scn = $max_frozen_scn, 0, 600;

# mysql租户  开启数据备份
ob1.mysql alter system backup database;
ob1.check_until_timeout select count(*) c from __all_backup_job_history where tenant_id = 1, 1, 600;
ob1.check_until_timeout select result from oceanbase.CDB_OB_BACKUP_JOB_HISTORY where tenant_id = $backup_mysql_tenant_id, 0, 10;
ob1.check_until_timeout select result from oceanbase.CDB_OB_BACKUP_TASK_HISTORY where tenant_id = $backup_mysql_tenant_id, 0, 10;

connect(backup_sys_conn, $backup_ip, root@sys, , oceanbase, $backup_port);
connection backup_sys_conn;
use oceanbase;
let $restore_path = $log_backup_dest,$data_backup_dest;
let $ls_id1 = deploy_get_value(ob1.select select ls_id from oceanbase.__all_virtual_ls where tenant_id = $backup_mysql_tenant_id and ls_id != 1, ls_id, 1);
ob1.mysql alter tenant $backup_mysql_tenant set primary_zone = 'z1';
ob1.check_until_timeout select count(*)>=1 from oceanbase.__all_virtual_ls_status where status = 'NORMAL' and tenant_id = $backup_mysql_tenant_id, 1, 100;
let $ls_id2 = deploy_get_value(ob1.select select max(ls_id) as ls_id from oceanbase.__all_virtual_ls where tenant_id = $backup_mysql_tenant_id, ls_id, 1);
let $create_scn1 = deploy_get_value(ob1.select select create_scn from oceanbase.__all_virtual_ls where tenant_id = $backup_mysql_tenant_id and ls_id = $ls_id1, create_scn, 1);
let $create_scn2 = deploy_get_value(ob1.select select create_scn from oceanbase.__all_virtual_ls where tenant_id = $backup_mysql_tenant_id and ls_id = $ls_id2, create_scn, 1);
sleep 3;
let $restore_checkpoint1 = `select ceil(min_restore_scn / 1000) as min_restore_scn from CDB_OB_BACKUP_SET_FILES where tenant_id = $backup_mysql_tenant_id`;
let $restore_checkpoint2 = `select ceil(UNIX_TIMESTAMP(current_timestamp(6)) * 1000000)`;
ob1.check_until_timeout select floor(checkpoint_scn / 1000) > '$restore_checkpoint1' from CDB_OB_ARCHIVELOG, 1, 600;
ob1.check_until_timeout select floor(checkpoint_scn / 1000) > '$restore_checkpoint2' from CDB_OB_ARCHIVELOG, 1, 600;
let $restore_checkpoint = $restore_checkpoint2;
let $restore_checkpoint = `select usec_to_time(floor(checkpoint_scn/1000)) from CDB_OB_ARCHIVELOG`;

# mysql租户  开启数据恢复
--disable_all_result_log
ob1.mysql alter system restore $restore_mysql_tenant from '$restore_path' until time='$restore_checkpoint' with 'pool_list=small_pool_2&primary_zone=z1';
--enable_all_result_log
ob1.check_until_timeout SELECT status FROM __all_tenant WHERE tenant_id = 1003, NORMAL, 200;
if ($multi_zone_flag == 0)
{
  ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = 1004 and restore_status = 0, 2, 600;
}
if ($multi_zone_flag == 1)
{
  ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = 1004 and restore_status = 0, 6, 600;
}
ob1.check_until_timeout select status FROM __all_tenant WHERE tenant_id = 1004, NORMAL, 200;
ob1.check_until_timeout select count(*) FROM __all_restore_job, 0, 200;
ob_connect(restore_mysql_conn, ob1.z1.obs0, root@$restore_mysql_tenant, , test, , USE_MYSQL_CONNECTOR);
connection restore_mysql_conn;
alter system set merger_check_interval = '10s';
# 使备租户进入持续同步模式 
ob1.mysql ALTER SYSTEM RECOVER STANDBY TENANT $restore_mysql_tenant UNTIL UNLIMITED;
