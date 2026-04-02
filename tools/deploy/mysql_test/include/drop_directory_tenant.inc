# 删除一个租户
# 需要指定租户名

# let $TENANT = XXX
# --source mysql_test/include/drop_tenant.inc

--disable_query_log
--disable_result_log

if ($DIR != '') {

# 连接默认租户删除dir

connection default;
--error 0, 2024
eval drop database link $DIR;

# 连接sys租户删除租户

connect (obsys, $OBMYSQL_MS0, admin, $OBMYSQL_PWD, test, $OBMYSQL_PORT);
connection obsys;

--disable_warnings
eval drop tenant if exists $TENANT force;
--enable_warnings

disconnect obsys;

}

--enable_result_log
--enable_query_log

