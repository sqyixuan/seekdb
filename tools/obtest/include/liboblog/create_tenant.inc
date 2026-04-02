# sys租户创建oblog_tt
# create tenant, 创建oblog_tt租户

let $ip=deploy_get_value(ob1.z1.obs0.ip);
let $port=deploy_get_value(ob1.z1.obs0.mysql_port);
#connect (oblog_conn_sys,$ip,admin@sys,admin,oceanbase,$port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

create resource unit oblog_box max_cpu 2, memory_size='20G';
create resource pool oblog_pool unit = 'oblog_box', unit_num = 1, zone_list = ('z1');
create tenant oblog_tt replica_num = 1, primary_zone='z1', resource_pool_list=('oblog_pool') set ob_tcp_invited_nodes='%';

sleep 2;

# 在oblog_tt租户下操作
connect (oblog_conn_tt,$ip,root@oblog_tt,,oceanbase,$port);
connection oblog_conn_tt;

let $mysql_tenant_name=oblog_tt;
let $original_conn = oblog_conn_tt;