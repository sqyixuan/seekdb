# sys租户创建oblog_tt
# create tenant, 创建oblog_tt租户

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

# can not create user database in sys tenant, create oblog_tt tenant
create resource unit mysql_oblog_box max_cpu 2, max_memory=2000000000, max_iops 10000, max_disk_size 5000000000, max_session_num 9223372036854775807, MIN_CPU=1, MIN_MEMORY=2000000000, MIN_IOPS=5000;
create resource pool mysql_oblog_pool unit = 'mysql_oblog_box', unit_num = 1, zone_list = ('z1');
create tenant oblog_tt replica_num = 1, primary_zone='z1', resource_pool_list=('mysql_oblog_pool') set ob_tcp_invited_nodes='%';

sleep 2;

# 在oblog_tt租户下操作
connect (oblog_conn_tt,$ip,root@oblog_tt,,oceanbase,$port);
connection oblog_conn_tt;
