
#--echo
#--echo ########### import time zone info ###########

--disable_query_log
--disable_result_log
--disable_warnings

ob_connect(conn_admin_tz, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);

connection conn_sys;
select count(*) c1 from oceanbase.__all_tenant_time_zone;
select count(*) c1 from oceanbase.__all_tenant_time_zone_name;
select count(*) c1 from oceanbase.__all_tenant_time_zone_transition;
select count(*) c1 from oceanbase.__all_tenant_time_zone_transition_type;
delete from oceanbase.__all_tenant_time_zone;
delete from oceanbase.__all_tenant_time_zone_name;
delete from oceanbase.__all_tenant_time_zone_transition;
delete from oceanbase.__all_tenant_time_zone_transition_type;

alter system change tenant oracle;
sleep 2;

--source include/liboblog/time_zone_info.sql

sleep 10;
disconnect conn_admin_tz;

--enable_result_log
--enable_warnings

