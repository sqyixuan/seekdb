# 创建一个oracle模式租户和一个用户
# 需要指定租户名、用户名、密码

# 使用示例
# let $TENANT = XXX
# let $USER = XXX
# let $PWD = XXX
# --source mysql_test/include/create_tenant.inc

--disable_query_log
--disable_result_log

if ($DBLINK != '') {

# 连接sys租户创建新租户

let $POOL = _pool;
let $POOL = $TENANT$POOL;
let $UNIT = _unit;
let $UNIT = $TENANT$UNIT;

connect (obsys, $OBMYSQL_MS0, admin, $OBMYSQL_PWD, test, $OBMYSQL_PORT);
connection obsys;

--disable_warnings
eval DROP TENANT if exists $TENANT FORCE;
eval DROP RESOURCE POOL if exists $POOL;
eval DROP RESOURCE UNIT if exists $UNIT;
--enable_warnings

eval CREATE RESOURCE UNIT $UNIT MAX_CPU=1, MEMORY_SIZE='2G';
eval CREATE RESOURCE POOL $POOL UNIT='$UNIT', UNIT_NUM=1, ZONE_LIST=('zone1');
eval CREATE TENANT $TENANT REPLICA_NUM=1, RESOURCE_POOL_LIST=('$POOL'), charset=gbk set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
--source mysql_test/include/check_tenant_sync.inc

disconnect obsys;

# 连接新租户创建新用户

let $__tenant_name__ = '$TENANT';
--source mysql_test/include/get_tenant_server.inc
connect (tenant_conn, $TE_SERVER_IP, sys@$TENANT, ,*NO-ONE*, $TE_SERVER_PORT);
connection tenant_conn;
--disable_query_log
--disable_result_log

--error 0,600
eval drop user $USER cascade;
eval create user $USER IDENTIFIED BY $PWD;
eval grant all on *.* to $USER WITH GRANT OPTION;
eval grant dba to $USER;

--error 0,600
eval drop user $USER2 cascade;
eval create user $USER2 IDENTIFIED BY $PWD;
eval grant all on *.* to $USER2 WITH GRANT OPTION;
eval grant dba to $USER2;

disconnect tenant_conn;

# 创建到新租户的连接

connect (dblink_conn, $TE_SERVER_IP, $USER@$TENANT, $PWD, *NO-ONE*, $TE_SERVER_PORT);
connect (normal_conn, $TE_SERVER_IP, $USER2@$TENANT, $PWD, *NO-ONE*, $TE_SERVER_PORT);

# 连接默认租户创建dblink

connection normal_conn;

--error 0, 2024
eval drop database link $DBLINK;
--replace_regex /[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\:[0-9]+/xxx:xxx/g
eval create database link $DBLINK connect to $USER@$TENANT identified by $PWD host '$TE_SERVER_IP:$TE_SERVER_PORT';

}

--enable_result_log
--enable_query_log

