## 
# owner      : xuanruo.zsr
# owner group: sql
# description: 兼容性升级公共步骤
##

ob1.proxy0.set extra_config=syslog_level=debug;
ob1.reboot;
sleep 5;

# 设置proxy租户内存规格为5G
ob1.mysql alter RESOURCE UNIT proxyunit max_memory=5368709120,min_memory=5368709120;

# 1. 修改配置项
ob1.mysql alter system set balancer_idle_time='10s';
ob1.mysql alter system set merger_check_interval='10s';
ob1.mysql alter system set resource_soft_limit=200;
ob1.mysql alter system set resource_hard_limit=200;

# 2. proxy同步集群状态的时间间隔
let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (conn_proxy,$ip,root@obproxy,,test,$port);
connect (conn_obtest,$ip,root@obtest,,*NO-ONE*,$port);
connect (conn_sys,$ip,root@sys,,oceanbase,$port);

alter proxyconfig set server_state_refresh_interval='1s';

# 3.数据准备
# 3.1 启动客户端
let $obs0ip=deploy_get_value(ob1.z1.obs0.ip);
let $obs0port=deploy_get_value(ob1.z1.obs0.mysql_port);
ob1.sh nohup java -jar jar/obupgrade-1.0-SNAPSHOT-jar-with-dependencies.jar -h $obs0ip -p $obs0port -tenant obtest &;
sleep 60;

# 4 合并
ob1.mysql alter system major freeze;
sleep 180;
ob1.check_until_timeout select info from __all_zone where zone='' and name='merge_status',IDLE, 1800;

ob1.z1.obs0.mysql set global ob_query_timeout=600000000; 
ob1.z1.obs0.mysql set global ob_trx_timeout=600000000; 
ob1.mysql_p alter system minor freeze tenant = all;
sleep 5;

# 5 打开升级配置, 升级前检查
ob1.upgrade_check_before;
ob1.mysql alter system begin upgrade;
sleep 2;

# 6.执行升级SQL
ob1.upgrade_commond_for_server pre;
sleep 5;

# 6.1 转储
ob1.mysql_p alter system minor freeze tenant = all;
sleep 60;

#6.2 强制汇报一次，规避bug18479035
ob1.mysql alter system report replica;
ob1.check_until_timeout select count(*) from __all_meta_table where quorum=-1,0,120;
ob1.check_until_timeout select count(*) from __all_root_table where quorum=-1,0,120;

# 7. 升级z1 rs在新server上
# 7.1 首先检查日志是同步状态
ob1.check_until_timeout select count(1) from __all_virtual_core_meta_table where role=1 and zone='z1',1,120;
ob1.check_until_timeout select count(1) from __all_virtual_server_stat,3,120;
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
connection conn_sys;
set @@session.ob_query_timeout = 600000000;
alter system stop zone 'z1';
ob1.check_until_timeout select info from __all_zone where zone='z1' and name='status',INACTIVE,20;
ob1.check_until_timeout select leader_count from __all_virtual_server_stat where zone = 'z1',0,20;
sleep 2;
ob1.z1.check_state_in_congest upgrade;

# 7.2 升级版本
ob1.z1.stop;
ob1.z1.upgrade;
ob1.z1.nstart;
ob1.check_until_timeout select count(*) from oceanbase.__all_server where zone = 'z1' and (start_service_time<=0 or status='INACTIVE'),0,600;
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
ob1.mysql alter system start zone 'z1';
sleep 2;
ob1.check_until_timeout select count(1) from __all_virtual_server_stat,3,120;
ob1.z1.check_state_in_congest active;
ob1.check_until_timeout select count(1) from __all_virtual_core_meta_table where role=1 and zone='z1',1,120;

# 7.3 转储
ob1.mysql_p alter system minor freeze tenant = all;
sleep 60;

# 8. 先升级z2 rs在老server上
# 8.1 首先检查日志是同步状态
ob1.check_until_timeout select count(1) from __all_virtual_core_meta_table where role=1 and zone='z1',1,120;
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
connection conn_sys;
set @saved_ob_query_timeout = @@session.ob_query_timeout;
set @@session.ob_query_timeout = 600000000;
alter system stop zone 'z2';
set @@session.ob_query_timeout = @saved_ob_query_timeout;
ob1.check_until_timeout select info from __all_zone where zone='z2' and name='status',INACTIVE,20;
ob1.check_until_timeout select leader_count from __all_virtual_server_stat where zone = 'z2',0,20;
sleep 2;
ob1.z2.check_state_in_congest upgrade;

# 8.2 升级版本
ob1.z2.stop;
ob1.z2.upgrade;
ob1.z2.nstart;
ob1.check_until_timeout select count(*) from oceanbase.__all_server where zone = 'z2' and (start_service_time<=0 or status='INACTIVE'),0,600;
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
ob1.mysql alter system start zone 'z2';
sleep 2;
ob1.check_until_timeout select count(1) from __all_virtual_server_stat,3,120;
ob1.z2.check_state_in_congest active;

# 8.3 转储
ob1.mysql_p alter system minor freeze tenant = all;
sleep 60;

# 9. 最后升级z3 
# 9.1 首先检查日志是同步状态
ob1.check_until_timeout select count(1) from __all_virtual_core_meta_table where role=1 and zone='z1',1,120;
ob1.check_until_timeout select count(1) from __all_virtual_server_stat,3,120;
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
connection conn_sys;
set @@session.ob_query_timeout = 600000000;
alter system stop zone 'z3';
ob1.check_until_timeout select info from __all_zone where zone='z3' and name='status',INACTIVE,20;
ob1.check_until_timeout select leader_count from __all_virtual_server_stat where zone = 'z3',0,20;
sleep 2;
ob1.z3.check_state_in_congest upgrade;

# 9.2 升级版本
ob1.z3.stop;
ob1.z3.upgrade;
ob1.z3.nstart;
ob1.check_until_timeout select count(*) from oceanbase.__all_server where zone = 'z3' and (start_service_time<=0 or status='INACTIVE'),0,600;
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
ob1.mysql alter system start zone 'z3';
sleep 2;
ob1.check_until_timeout select count(1) from __all_virtual_server_stat,3,120;
ob1.z3.check_state_in_congest active;
ob1.check_until_timeout select count(1) from __all_virtual_core_meta_table where role=1 and zone='z1',1,120;
sleep 5;

# 9.3 转储
ob1.mysql_p alter system minor freeze tenant = all;
sleep 60;

# 10. 执行升级后SQL
connection conn_sys;
set @@session.ob_query_timeout = 600000000;
alter system upgrade virtual schema;
sleep 5;
ob1.upgrade_commond_for_server post;
sleep 5;
alter system end upgrade;
sleep 5;
alter system run job 'root_inspection';
sleep 10;
ob1.upgrade_check_after;

# 12. 升级后检查
# 12.1 首先检查日志是同步状态,然后合并
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
ob1.mysql_p alter system minor freeze tenant = all;
sleep 60;
ob1.mysql alter system major freeze;
sleep 60;
ob1.check_until_timeout select info from __all_zone where zone='' and name='merge_status',IDLE,1800;

# 12.2 停止客户端
ob1.sh pkill -f obupgrade-1.0-SNAPSHOT-jar-with-dependencies.jar;
sleep 5;

# 12.3 重启
ob1.force_stop;
sleep 5;
ob1.nstart;
sleep 30;
ob1.check_until_timeout select count(1) from __all_virtual_core_meta_table where role=1 and zone='z1',1,600;
ob1.check_until_timeout SELECT CAST(COUNT(*) AS SIGNED) as cnt FROM __all_virtual_clog_stat WHERE is_in_sync = 0 and is_offline=0,0,600;
ob1.mysql_p alter system minor freeze tenant = all;
sleep 60;
ob1.mysql alter system major freeze;
sleep 60;
ob1.check_until_timeout select info from __all_zone where zone='' and name='merge_status',IDLE,1800;

# 检查mysqltest执行结果
ob1.sh_p grep 'ERROR' obupgrade.log;


