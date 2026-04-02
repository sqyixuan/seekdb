#############################################
#owner : @wanhong.wwh
#owner group: rs
#description: quick test
# 测试点：验证资源池变更
#############################################
connection conn_sys;
ob1.mysql alter system set _transfer_start_trans_timeout = '10s';
ob1.mysql alter system set _transfer_start_rpc_timeout = '10s';
ob1.mysql alter system set ls_gc_delay_time='0s' tenant=sys;
ob1.mysql alter system set ls_gc_delay_time='0s' tenant=all_user;
ob1.mysql alter system set ls_gc_delay_time='0s' tenant=all_meta;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id group by zone order by zone;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id - 1 group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id and zone in ("z1", "z3") group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id - 1 and zone in ("z1", "z3") group by zone order by zone;
eval ALTER RESOURCE POOL pool_1 ZONE_LIST = ('z1', 'z2');
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id group by zone  order by zone;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id - 1 group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id and zone in ("z1", "z2") group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id - 1 and zone in ("z1", "z2") group by zone order by zone;
eval ALTER RESOURCE TENANT mysql UNIT_NUM = 2;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_ls_status where tenant_id = $mysql_tenant_id and status = 'NORMAL', 4, 50;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id group by zone order by zone;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id - 1 group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id and zone in ("z1", "z2") group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id - 1 and zone in ("z1", "z2") group by zone order by zone;
ob1.check_until_timeout select count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status = 'ACTIVE', 4, 100;
ob1.check_until_timeout select count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status != 'ACTIVE', 0, 1000;
# 等待balance job执行完成
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_balance_job where tenant_id = $mysql_tenant_id, 0, 120;
ob1.check_until_timeout select count(*) from oceanbase.DBA_OB_LS where status != 'NORMAL', 0, 100;
eval ALTER RESOURCE TENANT mysql UNIT_NUM = 1 DELETE UNIT_GROUP = (1002);
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_ls_status where tenant_id = $mysql_tenant_id and status = 'NORMAL', 3, 100;
ob1.check_until_timeout select count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status = 'ACTIVE', 2, 100;
ob1.check_until_timeout select count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status != 'ACTIVE', 0, 1000;
# 等待balance job执行完成
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_balance_job where tenant_id = $mysql_tenant_id, 0, 120;
ob1.check_until_timeout select count(*) from oceanbase.DBA_OB_LS where status != 'NORMAL', 0, 100;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_unit where tenant_id in ($mysql_tenant_id, $mysql_tenant_id - 1), 4, 300;
eval ALTER RESOURCE TENANT mysql UNIT_NUM = 2;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_ls_status where tenant_id = $mysql_tenant_id and status = 'NORMAL', 4, 50;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id group by zone order by zone;
eval select zone, count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id - 1 group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id and zone in ("z1", "z2") group by zone order by zone;
eval select zone, count(*) as cnt from GV\$OB_UNITS where tenant_id = $mysql_tenant_id - 1 and zone in ("z1", "z2") group by zone order by zone;
ob1.check_until_timeout select count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status = 'ACTIVE', 4, 100;
ob1.check_until_timeout select count(*) as cnt from DBA_OB_UNITS where tenant_id = $mysql_tenant_id and status != 'ACTIVE', 0, 1000;
# 等待balance job执行完成
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_balance_job where tenant_id = $mysql_tenant_id, 0, 120;
ob1.check_until_timeout select count(*) from oceanbase.__all_virtual_ls_status where tenant_id = $mysql_tenant_id and status != 'NORMAL', 0, 100;
eval ALTER RESOURCE POOL pool_1 ZONE_LIST = ('z1', 'z3', 'z2');
