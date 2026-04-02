
#--echo
#--echo ########### import time zone info ###########

--disable_query_log
--disable_result_log
--disable_warnings

connect (conadmin,$OBMYSQL_MS0,admin@sys,admin,,$OBMYSQL_PORT);
connection conadmin;
let $ADMIN_SVR_IP = query_get_value('select SVR_IP from oceanbase.__all_virtual_server_stat',SVR_IP,1);
let $ADMIN_SQL_PORT = query_get_value('select SQL_PORT from oceanbase.__all_virtual_server_stat',SQL_PORT,1);

connect (conn_admin_tz, $ADMIN_SVR_IP,admin,$OBMYSQL_PWD,test,$ADMIN_SQL_PORT);
connection conn_admin_tz;
select count(*) c1 from oceanbase.__all_tenant_time_zone;
select count(*) c1 from oceanbase.__all_tenant_time_zone_name;
select count(*) c1 from oceanbase.__all_tenant_time_zone_transition;
select count(*) c1 from oceanbase.__all_tenant_time_zone_transition_type;
delete from oceanbase.__all_tenant_time_zone;
delete from oceanbase.__all_tenant_time_zone_name;
delete from oceanbase.__all_tenant_time_zone_transition;
delete from oceanbase.__all_tenant_time_zone_transition_type;
sleep 2;

####--source mysql_test/test_suite/time_zone/t/time_zone_info.sql
--source mysql_test/test_suite/mysql57_t/time_zone_info.sql

sleep 10;
disconnect conn_admin_tz;
connection default;

--enable_query_log
--enable_result_log
--enable_warnings
