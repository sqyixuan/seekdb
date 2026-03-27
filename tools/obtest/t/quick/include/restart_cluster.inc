#############################################
#owner : @msy164651
#owner group: rs
#description: quick test
# 测试点：验证集群重启
#############################################
let $stop_tstamp_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
create table t1_reboot (c1 int);
insert into t1_reboot values(1);
ob1.force_stop;
sleep 6;
ob1.nstart;
sleep 6;
--disable_query_log
# TODO@jingyu.cr: adjust this check for start_service_time after OBD ok
ob1.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_proxy_server_stat where start_service_time <= $stop_tstamp_usec, 0 ,180;
source include/check_schema_sync_for_restart.inc;
--enable_query_log
ob1.check_until_timeout select count(*) from __all_virtual_memory_info where mod_name like '%OB_MOD_DO_NOT_USE_ME%' and hold != 0, 0, 10;
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
select * from t1_reboot;
