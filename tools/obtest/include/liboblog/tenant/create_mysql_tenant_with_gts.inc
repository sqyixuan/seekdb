# sys租户创建cdc_mysql
# create tenant, 创建cdc_mysql租户

let $ip=deploy_get_value(ob1.z1.obs0.ip);
let $port=deploy_get_value(ob1.z1.obs0.mysql_port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

# can not create user database in sys tenant, create cdc_mysql tenant
# 最大内存20G, 保证可以频繁删除和添加分区
create resource unit mysql_oblog_box max_cpu 1, memory_size='5G';
create resource pool mysql_oblog_pool unit = 'mysql_oblog_box', unit_num = 1, zone_list = ('z1');
create tenant cdc_mysql replica_num = 1, primary_zone='z1', resource_pool_list=('mysql_oblog_pool') set ob_tcp_invited_nodes='%';

sleep 2;

# 在cdc_mysql租户下操作
connect (conn_obcdc_mysql,$ip,root@cdc_mysql,,oceanbase,$port);
connection conn_obcdc_mysql;
create user obcdc identified by "obcdc";
grant all on *.* to obcdc;
set autocommit=1;
let $mysql_tenant_name=cdc_mysql;
let $original_conn = conn_obcdc_mysql;
