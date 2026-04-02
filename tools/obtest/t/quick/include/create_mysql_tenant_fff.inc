#######################################################################################
#owner : wanhong.wwh
#owner group: rs
#description: quick test
# 测试点：
#   创建一个FFF的mysql租户，租户名为mysql
#########################################################################################


connection conn_sys;

set @@session.ob_query_timeout = 1000000000;
#mysql tenant
eval CREATE RESOURCE UNIT if not exists unit_1 MAX_CPU 2, MEMORY_SIZE='2G';
eval CREATE RESOURCE POOL if not exists pool_1 UNIT = 'unit_1', UNIT_NUM = 1;
eval CREATE TENANT mysql RESOURCE_POOL_LIST = ('pool_1'), primary_zone='z1', locality="F@z1, F@z2, F@z3" set ob_tcp_invited_nodes='%';
source include/check_schema_sync.inc;

let $mysql_tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.dba_ob_tenants where tenant_name = 'mysql', tenant_id, 1);
let $mysql_leader_ip_port=deploy_get_value(ob1.select select concat(svr_ip,':',svr_port) as host from oceanbase.cdb_ob_ls_locations where tenant_id = $mysql_tenant_id and ls_id =  1 and role like '%LEADER%' limit 1, host, 1);
let $mysql_leader_obs =deploy_get_value(ob1.get_obs_by_addr $mysql_leader_ip_port);
ob_connect(conn_mysql, $mysql_leader_obs, root@mysql, , test, , USE_MYSQL_CONNECTOR);