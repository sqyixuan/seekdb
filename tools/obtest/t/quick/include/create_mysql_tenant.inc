####################################################
#owner : zhaoziqian.zzq
#owner group: rs
#description: quick test
# 测试点：创建mysql租户，初始locality为“F@z1, F@z3”
####################################################
#验证创建租户
set @@session.ob_query_timeout = 1000000000;
#mysql tenant
eval CREATE RESOURCE UNIT unit_1 MAX_CPU 2, MEMORY_SIZE='3G';
eval CREATE RESOURCE POOL pool_1 UNIT = 'unit_1', UNIT_NUM = 1;
eval CREATE TENANT mysql RESOURCE_POOL_LIST = ('pool_1'), primary_zone='z1', locality="F@z1, F@z3" set ob_tcp_invited_nodes='%';
source include/check_schema_sync.inc;
# can't login meta tenant
--error 1235
ob_connect(conn_mysql_meta, ob1.z1.obs0, root@META\$1002, , test, , USE_MYSQL_CONNECTOR);
connection conn_sys;
let $mysql_tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.__all_tenant where tenant_name = 'mysql', tenant_id, 1);
# test cmd
alter system run job "root_inspection";
alter system upgrade virtual schema;
