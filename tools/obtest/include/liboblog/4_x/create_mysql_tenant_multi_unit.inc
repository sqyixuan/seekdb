# sys租户创建oblog_tt
# create tenant, 创建oblog_tt租户

ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, ,USE_MYSQL_CONNECTOR);

# 连接sys租户
connection conn_sys;

# can not create user database in sys tenant, create oblog_tt tenant
# 最大内存20G, 保证可以频繁删除和添加分区
if ($user_define_tenant_size == 1)
{
  eval create resource unit obcdc_mysql_box max_cpu $max_cpu, memory_size=$memory_size;
}
if ($user_define_tenant_size == 0)
{
  create resource unit obcdc_mysql_box max_cpu 2, memory_size='20G';
}
create resource pool obcdc_mysql_pool unit = 'obcdc_mysql_box', unit_num = 2, zone_list = ('z1');
create tenant obcdc_mysql replica_num = 1, primary_zone='z1', resource_pool_list=('obcdc_mysql_pool') set ob_tcp_invited_nodes='%';

sleep 2;

# 在oblog_tt租户下操作
ob_connect(conn_obcdc_mysql, ob1.z1.obs0, root@obcdc_mysql, , oceanbase, USE_MYSQL_CONNECTOR);
connection conn_obcdc_mysql;
create user obcdc identified by "obcdc";
grant all on *.* to obcdc;

let $mysql_tenant_name=obcdc_mysql;
let $original_conn = conn_obcdc_mysql;
