# sys租户创建oblog_tt
# create tenant, 创建oblog_tt租户

ob_connect(oblog_conn_sys,ob1.z1.obs0,root@sys,,oceanbase,, USE_MYSQL_CONNECTOR);

# 连接sys租户
connection oblog_conn_sys;

# 清理资源
--disable_warnings
drop tenant if exists oblog_tt;
--disable_warnings
drop resource pool if exists oblog_pool;
--disable_warnings
drop resource unit if exists oblog_box;

# can not create user database in sys tenant, create oblog_tt tenant
create resource unit oblog_box max_cpu 2, memory_size='10G';
create resource pool oblog_pool unit = 'oblog_box', unit_num = 1, zone_list = ('z1', 'z2', 'z3');
#create tenant oblog_tt replica_num = 1, primary_zone='z1', resource_pool_list=('oblog_pool') set ob_tcp_invited_nodes='%';
create tenant oblog_tt replica_num = 1, primary_zone='z1', resource_pool_list=('oblog_pool') set ob_tcp_invited_nodes='%';
source include/liboblog/check_schema_fast_sync.inc;
sleep 2;

# 在oblog_tt租户下操作
ob_connect(oblog_conn_tt,ob1.z1.obs0,root@oblog_tt,,oceanbase, , USE_MYSQL_CONNECTOR);
connection oblog_conn_tt;
