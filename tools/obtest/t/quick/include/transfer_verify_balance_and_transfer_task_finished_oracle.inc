#######################################################################################
#owner : wangzhennan.wzn
#owner group: rs
#description: quick test
# 测试点：
#   等待负载均衡和transfer任务执行完成
#########################################################################################

connection conn_oracle;

# 等待balance job执行完成
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_BALANCE_JOB_HISTORY where tenant_id = $oracle_tenant_id, 1, 300;

# 等待balance job执行完成
ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_BALANCE_JOBS where tenant_id = $oracle_tenant_id, 0, 60;

ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_LS where tenant_id = $oracle_tenant_id and status != 'NORMAL', 0, 100;
# 查询均衡结果
echo ====== show ALL LS status
SELECT LS_ID, STATUS, PRIMARY_ZONE FROM SYS.DBA_OB_LS;
echo ====== show ALL <table, partitoin, tablet, ls> info
SELECT DISTINCT TABLE_ID, TABLE_NAME, PARTITION_NAME, TABLET_ID, LS_ID FROM SYS.DBA_OB_TABLE_LOCATIONS WHERE TABLE_ID >= 500000 ORDER BY TABLE_ID, TABLET_ID;
echo ====== show ALL balance job
SELECT JOB_TYPE, TARGET_UNIT_NUM, TARGET_PRIMARY_ZONE_NUM, STATUS FROM SYS.DBA_OB_BALANCE_JOB_HISTORY;
echo ====== show ALL balance task
SELECT task_type, src_ls, dest_ls, part_list, finished_part_list, part_count, finished_part_count, ls_group_id, status, parent_list FROM SYS.DBA_OB_balance_task_history;
echo ====== show ALL transfer task
SELECT src_ls, dest_ls, part_list, part_count, not_exist_part_list, lock_conflict_part_list, table_lock_tablet_list, tablet_list, tablet_count, status, result FROM SYS.DBA_OB_transfer_task_history;

# 删除历史记录
connection conn_sys;
alter system change tenant oracle;
eval DELETE FROM oceanbase.__all_balance_job_history;
eval DELETE FROM oceanbase.__all_balance_task_history;
eval DELETE FROM oceanbase.__all_transfer_task_history;
alter system change tenant sys;
