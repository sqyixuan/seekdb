#######################################################################################
#owner : wanhong.wwh
#owner group: rs
#description: quick test
# 测试点：
#   等待负载均衡执行完成，验证transfer执行结果
#########################################################################################

# 验证均衡任务和transfer任务完成
source t/quick/include/transfer_verify_balance_and_transfer_task_finished.inc;

# 验证query和DML
# 验证非分区表的query和DML
source t/quick/include/transfer_verify_nonpart_table_query_and_dml.inc;
# 验证分区表的query和DML
source t/quick/include/transfer_verify_part_table_query_and_dml.inc;


# 集群重启
--echo ===============================================================
--echo =================     restart cluster    ======================
--echo ===============================================================
--echo test restart cluster begin
source t/quick/include/restart_cluster.inc;
--echo test restart cluster finish
ob_connect(conn_sys, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
let $mysql_tenant_id = deploy_get_value(ob1.select select tenant_id from oceanbase.dba_ob_tenants where tenant_name = 'mysql', tenant_id, 1);
let $mysql_leader_ip_port=deploy_get_value(ob1.select select concat(svr_ip,':',svr_port) as host from oceanbase.cdb_ob_ls_locations where tenant_id = $mysql_tenant_id and ls_id =  1 and role like '%LEADER%' limit 1, host, 1);
let $mysql_leader_obs =deploy_get_value(ob1.get_obs_by_addr $mysql_leader_ip_port);
ob_connect(conn_mysql, $mysql_leader_obs, root@mysql, , test, , USE_MYSQL_CONNECTOR);

# 验证mysql租户下的switch replica leader
--echo ===============================================================
--echo ==============     switch replica leader    ===================
--echo ===============================================================
--echo test switch mysql replica leader begin
source t/quick/include/switch_mysql_replica_leader.inc;
--echo test switch mysql replica leader finish

# 验证非分区表的query和DML
source t/quick/include/transfer_verify_nonpart_table_query_and_dml.inc;
# 验证分区表的query和DML
source t/quick/include/transfer_verify_part_table_query_and_dml.inc;
