###############################################################
#owner : @shouju.zyp
#owner group: rs
#description: quick test
#测试点：验证存在mysql和oracle租户，重启集群，执行root_inspection
###############################################################

ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_sys;

# eval CREATE RESOURCE UNIT IF NOT EXISTS oracle_unit1 MAX_CPU 2, MEMORY_SIZE='3G';
# eval CREATE RESOURCE POOL IF NOT EXISTS oracle_pool1 UNIT = 'unit_1', UNIT_NUM = 1;
# eval CREATE TENANT IF NOT EXISTS oracle_tenant1 RESOURCE_POOL_LIST = ('oracle_pool1'), primary_zone='z1', locality="F@z1, F@z2, F@z3" set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
# source include/check_schema_sync.inc;

# 验证集群里有mysql和oracle租户
# 如果前面有删租户的case，需要在这里重新创建租户
ob1.check_until_timeout select count(*)>0 a from __all_tenant where tenant_id % 2 = 0 and compatibility_mode = 0, 1, 10;
ob1.check_until_timeout select count(*)>0 a from __all_tenant where tenant_id % 2 = 0 and compatibility_mode = 1, 1, 10;

ob1.force_stop;
ob1.nstart;

source include/check_schema_sync_for_restart.inc;

ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection conn_sys;

set @@session.ob_query_timeout = 1000000000;
alter system run job 'root_inspection';
