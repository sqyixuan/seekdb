#############################################
#owner : @wangzhennan.wzn
#owner group: rs
#description: quick test
# 测试点：验证server/zone运维操作
#############################################

--disable_query_log
connection conn_sys;

# 为了验证stop zone, isolate server会切主，我们先把系统租户的primary zone设置为z3
# 因为mysql租户目前是 "F@z1,F@z2", z1和z2上不能执行真正的stop操作

# 切RS到z3
let $sys_ls_obs_z3 = deploy_get_value(ob1.select select concat(svr_ip,":",svr_port) as obs from __all_virtual_core_meta_table where zone = 'z3', obs, 1);
ob1.mysql alter system switch replica leader ls = 1 server = '$sys_ls_obs_z3' tenant = 'sys';

# 验证系统租户的leader在z3上
ob1.check_until_timeout select count(*) from DBA_OB_LS_LOCATIONS where zone='z3' and role='LEADER', 1, 30;

# isolate server and start server
let $z3_svr_ip = deploy_get_value(ob1.select select svr_ip from DBA_OB_LS_LOCATIONS where zone='z3', svr_ip, 1);
let $z3_svr_port = deploy_get_value(ob1.select select svr_port from DBA_OB_LS_LOCATIONS where zone='z3', svr_port, 1);

echo ======== alter system isolate server;
eval alter system isolate server "$z3_svr_ip:$z3_svr_port";
# 验证STOP_TIME
ob1.check_until_timeout select count(*) from DBA_OB_SERVERS where svr_ip = '$z3_svr_ip' and svr_port = '$z3_svr_port' and stop_time is not null, 1, 30;
# 验证系统租户的leader切走
ob1.check_until_timeout select count(*) from DBA_OB_LS_LOCATIONS where zone='z3' and role='LEADER', 0, 30;

echo =======  alter system start server;
eval alter system start server "$z3_svr_ip:$z3_svr_port";
# 验证STOP_TIME
ob1.check_until_timeout select count(*) from DBA_OB_SERVERS where svr_ip = '$z3_svr_ip' and svr_port = '$z3_svr_port' and stop_time is null, 1, 30;

# 验证stop zone
# 由于目前mysql租户是：F@z1,F@z2, 所以，直接stop z1或者z2 会报错不满足多数派
--enable_query_log
echo =======  alter system stop zone "z1";
--error 4179
eval alter system stop zone "z1";

# stop z3是不会报错的
echo =======  alter system stop zone "z3";
eval alter system stop zone "z3";
ob1.check_until_timeout select count(*) from DBA_OB_ZONES where status = 'INACTIVE' and zone = 'z3', 1, 30;

# 采用CDB视图，检查z3上所有日志流不能是leader
ob1.check_until_timeout select count(*) from CDB_OB_LS_LOCATIONS where zone='z3' and role='LEADER', 0, 30;

eval alter system start zone "z3";
ob1.check_until_timeout select count(*) from DBA_OB_ZONES where status = 'ACTIVE' and zone = 'z3', 1, 30;

# z3 start之后sys租户leader受primary_zone影响切回z3
ob1.check_until_timeout select count(*) from DBA_OB_LS_LOCATIONS where zone='z3' and role='LEADER', 1, 30;

# 确认RS上任成功
ob1.check_until_timeout select count(*) from __all_virtual_core_meta_table where zone = 'z3' and role = 1, 1, 60;