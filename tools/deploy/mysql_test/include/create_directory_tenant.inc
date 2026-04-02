# 创建一个oracle模式租户和一个用户
# 需要指定租户名、用户名、密码

# 使用示例
# let $TENANT = XXX
# let $USER = XXX
# let $PWD = XXX
# let $MEMORY_SIZE = XXX
# --source mysql_test/include/create_tenant.inc

--disable_query_log
--disable_result_log

if ($DIR != '') {

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
--disable_query_log
eval CREATE RESOURCE UNIT $UNIT MAX_CPU=1, MEMORY_SIZE='$MEMORY_SIZE';
eval CREATE RESOURCE POOL $POOL UNIT='$UNIT', UNIT_NUM=1, ZONE_LIST=('zone1');
if ($SECURE_FILE_PRIV != '') {
eval CREATE TENANT $TENANT REPLICA_NUM=1, RESOURCE_POOL_LIST=('$POOL'), charset=gbk set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle', secure_file_priv='$SECURE_FILE_PRIV';
}
if ($SECURE_FILE_PRIV == '') {
eval CREATE TENANT $TENANT REPLICA_NUM=1, RESOURCE_POOL_LIST=('$POOL'), charset=gbk set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
}
--source mysql_test/include/check_tenant_sync.inc
--enable_query_log

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

disconnect tenant_conn;

# 创建到新租户的连接

connect (dir_conn, $TE_SERVER_IP, $USER@$TENANT, $PWD, *NO-ONE*, $TE_SERVER_PORT);

}

--enable_result_log
--enable_query_log

