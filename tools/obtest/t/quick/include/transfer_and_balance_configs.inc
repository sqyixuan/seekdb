#####################
# owner : wangzhennan.wzn
# owner group : RS
# description : test transfer and balance configs
#####################

ob1.mysql alter system set _transfer_start_trans_timeout = '60s';
ob1.mysql alter system set debug_sync_timeout = '1000s';
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_sys;
set @@session.ob_query_timeout = 1000000000;

let $mysql_tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name = 'mysql', tenant_id, 1);
let $mysql_leader_ip_port=deploy_get_value(ob1.select select concat(svr_ip,':',svr_port) as host from __all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id =  1 and role = 1 limit 1, host, 1);
let $mysql_leader_obs =deploy_get_value(ob1.get_obs_by_addr $mysql_leader_ip_port);
ob_connect(conn_mysql, $mysql_leader_obs, root@mysql, , test, , USE_MYSQL_CONNECTOR);

--echo ################################################
--echo # 1. test balance related modify tenant commands
connection conn_mysql;
alter system set enable_rebalance='false';
ob1.check_until_timeout select count(*) from GV\$OB_PARAMETERS where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value != 'false', 0, 5;
ob1.check_until_timeout select count(*) from __all_virtual_tenant_parameter_info where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value = 'true', 0, 5;

--echo ##
--echo # 1.1 test modify primary_zone
connection conn_mysql;
# modify to the same primary_zone is allowed
alter tenant mysql primary_zone = 'z1,z2';
alter tenant mysql primary_zone = 'z2,z1';
alter tenant mysql primary_zone = 'z1,z2;z3';
alter tenant mysql primary_zone = 'z2,z1;z3';
--error 4179
alter tenant mysql primary_zone = 'z1';
--error 4179
alter tenant mysql primary_zone = 'z1,z3';
--error 4179
alter tenant mysql primary_zone = 'z1,z2,z3';
--error 4179
alter tenant mysql primary_zone = RANDOM;
--error 1210
alter tenant mysql primary_zone = 'z1,z1,z2';

--echo ##
--echo # 1.2 test modify unit_num
connection conn_sys;
# modify to the same unit_num is allowed
alter resource tenant mysql unit_num = 1;
--error 4179
alter resource tenant mysql unit_num = 2;

--echo ##
--echo # 1.3 test modify locality
connection conn_sys;
# modify to the same locality is allowed
alter tenant mysql locality = 'F@z1,F@z2,F@z3';
# locality remove primary_zone is not allowed
--error 4179
alter tenant mysql locality = 'F@z2,F@z3';
--error 4179
alter tenant mysql locality = 'F@z1,F@z3';
--error 4179
alter tenant mysql locality = 'F@z3';
# alter tenant locality without primary_zone is allowed
alter tenant mysql locality = 'F@z1,F@z2';
ob1.check_until_timeout select count(*) = 2 from __all_virtual_log_stat where tenant_id = $mysql_tenant_id and ls_id = 1, 1, 120;

# change primary_zone to verify modify locality
connection conn_mysql;
alter system set enable_rebalance='true';
ob1.check_until_timeout select count(*) from GV\$OB_PARAMETERS where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value != 'true', 0, 5;
ob1.check_until_timeout select count(*) from __all_virtual_tenant_parameter_info where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value = 'false', 0, 5;
alter tenant mysql primary_zone = 'RANDOM';
ob1.check_until_timeout select count(*) from __all_virtual_balance_job where tenant_id = $mysql_tenant_id, 0, 300;
alter system set enable_rebalance='false';
ob1.check_until_timeout select count(*) from GV\$OB_PARAMETERS where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value != 'false', 0, 5;
ob1.check_until_timeout select count(*) from __all_virtual_tenant_parameter_info where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value = 'true', 0, 5;

ob1.check_until_timeout select count(*) from __all_tenant where tenant_id = $mysql_tenant_id and primary_zone = 'RANDOM' and locality = 'FULL{1}@z1, FULL{1}@z2', 1, 10;

# locality add primary_zone is not allowed
connection conn_sys;
--error 4179
alter tenant mysql locality = 'F@z1,F@z2,F@z3';
--error 4179
alter tenant mysql locality = 'F@z1';

--echo ##
--echo # 1.4 balancer_idle_time can't longer than partition_balance_schedule_interval
--error 1210
alter system set balancer_idle_time = '3h' tenant = 'mysql';
--error 1210
alter system set partition_balance_schedule_interval = '1s' tenant = 'mysql';
connection conn_mysql;
--error 1210
alter system set balancer_idle_time = '3h';
--error 1210
alter system set partition_balance_schedule_interval = '1s';
# normal value
alter system set balancer_idle_time = '10s';
alter system set partition_balance_schedule_interval = '2h';

--echo ##
--echo # 1.4 commands are normal when enable rebalance
connection conn_mysql;
alter system set enable_rebalance='true';
ob1.check_until_timeout select count(*) from GV\$OB_PARAMETERS where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value != 'true', 0, 5;
ob1.check_until_timeout select count(*) from __all_virtual_tenant_parameter_info where tenant_id = $mysql_tenant_id and name = 'enable_rebalance' and value = 'false', 0, 5;

# 保证unit gc完成
ob1.check_until_timeout select count(*) from gv\$ob_units where tenant_id in ($mysql_tenant_id, $mysql_tenant_id - 1), 6, 300;

connection conn_sys;
alter resource tenant mysql unit_num = 2;
ob1.check_until_timeout select count(*) = 6 from gv\$ob_units where tenant_id = $mysql_tenant_id, 1, 300;
alter resource tenant mysql unit_num = 1;
ob1.check_until_timeout select count(*) = 3 from gv\$ob_units where tenant_id = $mysql_tenant_id, 1, 300;
alter tenant mysql locality = 'F@z1,F@z2,F@z3';
ob1.check_until_timeout select count(*) = 3 from __all_virtual_log_stat where tenant_id = $mysql_tenant_id and ls_id = 1, 1, 120;

let $mysql_tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name = 'mysql', tenant_id, 1);
let $mysql_leader_ip_port=deploy_get_value(ob1.select select concat(svr_ip,':',svr_port) as host from __all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id =  1 and role = 1 limit 1, host, 1);
let $mysql_leader_obs =deploy_get_value(ob1.get_obs_by_addr $mysql_leader_ip_port);
ob_connect(conn_mysql, $mysql_leader_obs, root@mysql, , test, , USE_MYSQL_CONNECTOR);
connection conn_mysql;
alter tenant mysql primary_zone = 'z1,z2,z3';
alter tenant mysql primary_zone = 'z1,z2';
alter tenant mysql primary_zone = RANDOM;

--echo ##
--echo # 1.5 alter primary_zone/locality for sys tenant are not affected
connection conn_sys;
alter system set enable_rebalance='false';
ob1.check_until_timeout select count(*) from GV\$OB_PARAMETERS where tenant_id = 1 and name = 'enable_rebalance' and value = 'false', 6, 5;
ob1.check_until_timeout select count(*) from __all_virtual_tenant_parameter_info where tenant_id = 1 and name = 'enable_rebalance' and value = 'true', 0, 5;
alter tenant sys primary_zone = 'z2';
alter tenant sys primary_zone = RANDOM;
alter tenant sys locality = 'F@z1, F@z2';
alter tenant sys locality = 'F@z1, F@z2, F@z3';

--echo ##
--echo # 1.5 test _transfer_task_tablet_count_threshold

--echo # 1.5.1 limit 0 < _transfer_task_tablet_count_threshold <= OB_MAX_TRANSFER_BINDING_TABLET_CNT
connection conn_sys;
alter system set _transfer_task_tablet_count_threshold = 518 tenant mysql;
--error 4147
alter system set _transfer_task_tablet_count_threshold = 0 tenant mysql;
--error 1210
alter system set _transfer_task_tablet_count_threshold = 519 tenant mysql;

--echo # 1.5.2 verify that a partition with max aux tables can be transferred
ob1.check_until_timeout select count(*) from CDB_OB_BALANCE_JOBS where tenant_id = $mysql_tenant_id, 0, 100;
connection conn_mysql;
# create table with 128 index and lob
--disable_query_log
create table t_with_max_index (c1 int primary key, c2 longtext) partition by hash(c1) partitions 2;
let $i = 1;
while ($i <= 128)
{
  eval create index idx_t_with_max_index_$i on t_with_max_index(c1);
  inc $i;
}
--enable_query_log
let $t_with_max_index_id = deploy_get_value(ob1.select select table_id from oceanbase.__all_virtual_table where table_name = 't_with_max_index', table_id, 1);
# alter primary_zone to trigger transfer task
connection conn_mysql;
alter tenant mysql primary_zone = 'z1';
ob1.check_until_timeout select count(*) from CDB_OB_BALANCE_JOBS where tenant_id = $mysql_tenant_id, 1, 60;
ob1.check_until_timeout select count(*) from CDB_OB_BALANCE_JOBS where tenant_id = $mysql_tenant_id, 0, 100;
# verify transfer success
connection conn_mysql;
eval select dest_ls, tablet_count, status, result from oceanbase.DBA_OB_TRANSFER_TASK_HISTORY where part_list like "%$t_with_max_index_id%";

--echo drop tenants
connection conn_sys;
set recyclebin = off;
drop tenant mysql force;