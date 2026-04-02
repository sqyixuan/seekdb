# sys tenant
connection conn_sys;

create resource unit oblog_box max_cpu 2, max_memory=1073741824, max_iops 10000, max_disk_size 5000000000, max_session_num 9223372036854775807, MIN_CPU=1, MIN_MEMORY=1073741824, MIN_IOPS=5000;
create resource pool oblog_pool unit = 'oblog_box', unit_num = 1;
# 创建空租户，整个升级过程不做任何DDL，用于验证添加空租户
create tenant oblog_tt_empty replica_num = 1, primary_zone='z1', resource_pool_list=('oblog_pool') set ob_tcp_invited_nodes='%';
