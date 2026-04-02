# EN_BACKUP_SYS_META_TASK_FAILED=190
# EN_BACKUP_SYS_TABLET_TASK_FAILED = 191,
# EN_BACKUP_DATA_TABLET_MINOR_SSTABLE_TASK_FAILED = 192,
# EN_BACKUP_DATA_TABLET_MAJOR_SSTABLE_TASK_FAILED = 193,
# EN_BACKUP_BUILD_LS_LEVEL_INDEX_TASK_FAILED = 194,
# EN_BACKUP_BUILD_TENANT_LEVEL_INDEX_TASK_FAILED = 195,
# EN_BACKUP_PREFETCH_BACKUP_INFO_FAILED = 196,
# EN_BACKUP_COMPLEMENT_LOG_TASK_FAILED = 197,
# EN_BACKUP_USER_META_TASK_FAILED = 198

ob1=OBI(cluster=1, proxy_num=0);

source include/mini_ob_config.inc;
ob1.z1.obs0.set extra_config = "cache_wash_threshold='1G',_ob_minor_merge_schedule_interval='3s'";

let $backup_ip       = deploy_get_value(ob1.z1.obs0.ip);
let $backup_port     = deploy_get_value(ob1.z1.obs0.mysql_port);
let $backup_dest = deploy_get_value(ob1.get_backup_dest);
let $backup_oracle_tenant   = backup_oracle_tenant;
let $backup_mysql_tenant    = backup_mysql_tenant;
let $log_backup_dest = deploy_get_value(ob1.get_log_backup_dest $backup_oracle_tenant);
let $data_backup_dest = deploy_get_value(ob1.get_data_backup_dest $backup_oracle_tenant);

# 必须先stop掉老的，删除才能有效，不然可能会有残留。
ob1.force_stop;
ob1.clean_backup_dest;
ob1.reboot;

# 固定sys租户规格
ob1.mysql alter resource unit sys_unit_config memory_size='2G';

connect(backup_sys_conn, $backup_ip, root@sys,,oceanbase, $backup_port);
connection backup_sys_conn;
use oceanbase;
source sql/physical_backup/prepare_single_zone_mini_resource.sql;

ob1.mysql alter system set _backup_idle_time = '10s';
ob1.mysql alter system set enable_async_syslog=false;

# create oracle tenant, test create tenant before major freeze
ob1.create_tenant CREATE TENANT $backup_oracle_tenant RESOURCE_POOL_LIST = ('small_pool'), primary_zone = "z1" set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
let $backup_oracle_tenant_id = query_get_value(select tenant_id from oceanbase.__all_tenant where tenant_name='$backup_oracle_tenant', tenant_id, 1);

#创建tablet
ob_connect(sys_conn_oracle, ob1.z1.obs0, SYS@$backup_oracle_tenant, ,SYS, , USE_OB_CONNECTOR);
set global wait_timeout = 288000;

--disable_all_result_log
eval alter system set log_archive_dest='location=$log_backup_dest';
eval alter system set data_backup_dest='$data_backup_dest';
--enable_all_result_log
create user test IDENTIFIED BY "test";
grant all on *.* to test WITH GRANT OPTION;
grant dba to test;
ob_connect(backup_oracle_conn, ob1.z1.obs0,TEST@$backup_oracle_tenant,test,test,,USE_OB_CONNECTOR);
connection backup_oracle_conn;

create table t1(a int, b int, c int, CONSTRAINT "c1" PRIMARY KEY ("A"));
insert into t1(a, b) values(1, 1);

connection backup_sys_conn;

#开启日志备份
ob1.mysql alter system archivelog;
ob1.check_until_timeout select status from oceanbase.CDB_OB_ARCHIVELOG where tenant_id=$backup_oracle_tenant_id, DOING, 600;

ob1.mysql alter system major freeze tenant = sys;
ob1.mysql alter system major freeze tenant = all_user;
ob1.mysql alter system major freeze tenant = all_meta;
sleep 10;
ob1.check_until_timeout select (frozen_scn - last_scn) as result from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=$backup_oracle_tenant_id, 0, 600;
