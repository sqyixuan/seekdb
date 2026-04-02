####################################################
#owner : yulan.way
#owner group: qa
#description: quick test
# 测试点：创建1F的租户
####################################################
#验证创建租户
connection conn_sys;
set @@session.ob_query_timeout = 1000000000;
#创建单副本的租户
#mysql租户
eval CREATE RESOURCE UNIT unit_2 MAX_CPU 2, MEMORY_SIZE='2G';
eval CREATE RESOURCE POOL pool_2 UNIT = 'unit_2', UNIT_NUM = 1;
eval CREATE TENANT tt1 RESOURCE_POOL_LIST = ('pool_2'), primary_zone='z1', locality="F@z1" set ob_tcp_invited_nodes='%';
source include/check_schema_sync.inc;
ob_connect(conn_tt1, ob1.z1.obs1, root@tt1, , test, , USE_MYSQL_CONNECTOR);
create table t1(c1 int);
#oracle租户
connection conn_sys;
eval CREATE RESOURCE POOL pool_3 UNIT = 'unit_2', UNIT_NUM = 1, zone_list=('z1');
eval CREATE TENANT tt2 RESOURCE_POOL_LIST = ('pool_3'), primary_zone='z1', locality="F@z1" set ob_tcp_invited_nodes='%',ob_compatibility_mode='oracle';;
source include/check_schema_sync.inc;
ob_connect(conn_tt2, ob1.z1.obs0, SYS@tt2, , SYS, , USE_OB_CONNECTOR);
create table t1(c1 int);
