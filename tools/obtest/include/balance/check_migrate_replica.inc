####33333
####case_index = 2;

##清空所有replica, 保持环境干净 
let $count = $case_index; 
while ($count > 0)
{
  ob1.mysql delete from oceanbase.__all_virtual_meta_table where tenant_id = 1000 + $count;
  dec $count;
}
ob1.mysql alter system set resource_hard_limit=900;
ob1.mysql alter system set enable_rebalance = 'false';
ob1.mysql alter system set enable_rereplication = 'false';
ob1.mysql alter system set debug_sync_timeout = '100s';
ob1.mysql alter system set enable_auto_leader_switch = 'False';
sleep 4;

let $cpu = 1;
let $memory = 1073741824;
let $min_memory = 1073741824;
let $disk = 536870912;
let $iops = 100000;
let $session = 64;

ob1.mysql CREATE RESOURCE UNIT unit_$case_index MAX_CPU $cpu, MAX_MEMORY $memory, MAX_IOPS $iops, MAX_DISK_SIZE $disk, MAX_SESSION_NUM $session, MIN_CPU=1, MIN_MEMORY=$min_memory, MIN_IOPS=128;
ob1.mysql CREATE RESOURCE POOL pool_$case_index UNIT = 'unit_$case_index', UNIT_NUM = 3, ZONE_LIST = ('z1', 'z2','z3', 'z4', 'z5');
ob1.mysql CREATE TENANT tenant_$case_index RESOURCE_POOL_LIST = ('pool_$case_index'), locality='F@z1,F@z2,F@z3,F@z4,R@z4,F@z5,R@z5';

--disable_warnings
--disable_query_log
--disable_result_log
sleep 5;
--error 0,1064
ob1.mysql alter tenant tenant_$case_index set variables ob_tcp_invited_nodes='%';
--enable_warnings
--enable_query_log
--enable_result_log


# login as new tenant and create a table
let $obs=deploy_get_value(ob1.z1.obs0.ip);
let $port =deploy_get_value(ob1.z1.obs0.mysql_port);
sleep 10;
connect(conn1,$obs,root@tenant_$case_index,,,$port);
connection conn1;
CREATE DATABASE obtest;
use obtest;
create table test_2 (pk int primary key);

let $table_id = deploy_get_value(ob1.select select table_id from __all_virtual_table where table_name = "test_2", table_id, 1);
## 在z4上面做一次F类型迁移，迁到没有副本的那台机器上；
###查找z4上空闲的机器；
let $dest_ip = deploy_get_value(ob1.select select svr_ip, svr_port from __all_server where zone='z4' except select svr_ip,  svr_port from __all_virtual_meta_table where zone='z4', svr_ip, 1);
let $dest_port = deploy_get_value(ob1.select select svr_ip, svr_port from __all_server where  zone='z4' except select svr_ip,  svr_port from __all_virtual_meta_table where zone='z4', svr_port, 1);
###查找F
let $source_ip = deploy_get_value(ob1.select select svr_ip from __all_virtual_meta_table where zone='z4' and replica_type=0, svr_ip, 1);
let $source_port = deploy_get_value(ob1.select select svr_port from __all_virtual_meta_table where zone='z4' and replica_type=0, svr_port, 1);

ob1.mysql alter system move replica partition_id='0%1@$table_id' source='$source_ip:$source_port' destination = '$dest_ip:$dest_port';

##check result
ob1.check_until_timeout select count(*) c from __all_virtual_meta_table where svr_ip='$dest_ip' and svr_port=$dest_port and replica_type = 0, 1, 120;
let $count = deploy_get_value(ob1.select select count(*) c from __all_virtual_meta_table where role = 1 and member_list like '%$dest_ip:$dest_port%', c, 1);
if ($count == 1) 
{
  echo "add dest server found in member list";
}
let $count = deploy_get_value(ob1.select select count(*) c from __all_virtual_meta_table where role = 1 and  member_list like '%$source_ip:$source_port%', c, 1);
if ($count == 0) 
{
  echo "remove source server found in member list";
}
#ob1.check_until_timeout select count(*) c from __all_virtual_meta_table where svr_ip='$source_ip' and svr_port=$source_port, 0, 120;

## 在z5上面做一次R类型迁移，迁到没有副本的那台机器上；
let $dest_ip = deploy_get_value(ob1.select select svr_ip, svr_port from __all_server where zone='z5'  except select svr_ip,  svr_port from __all_virtual_meta_table where zone='z5', svr_ip, 1);
let $dest_port = deploy_get_value(ob1.select select svr_ip, svr_port from __all_server where  zone='z5' except select svr_ip,  svr_port from __all_virtual_meta_table where zone='z5', svr_port, 1);
###查找R
let $source_ip = deploy_get_value(ob1.select select svr_ip from __all_virtual_meta_table where zone='z5' and replica_type=16, svr_ip, 1);
let $source_port = deploy_get_value(ob1.select select svr_port from __all_virtual_meta_table where zone='z5' and replica_type=16, svr_port, 1);

ob1.mysql alter system move replica partition_id='0%1@$table_id' source='$source_ip:$source_port' destination = '$dest_ip:$dest_port';

##check result
ob1.check_until_timeout select count(*) c from __all_virtual_meta_table where svr_ip='$dest_ip' and svr_port=$dest_port and replica_type = 16, 1, 120;
#ob1.check_until_timeout select count(*) c from __all_virtual_meta_table where svr_ip='$source_ip' and svr_port=$source_port, 0, 120;

