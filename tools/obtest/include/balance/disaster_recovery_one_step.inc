## this inc is only used by cangming_C_edge_cases.test
## before include this inc, set debug_sync BEFORE_TRY_DISASTER_RECOVERY as signal1, and BEFORE_SEND_DRTASK_RPC as signal2
## and include with session sys_conn.
## make sure only ONE DR-task will be generated in one round.

--echo one step
# read ls replica task plan
eval select tenant_id, ls_id, task_type, priority, target_replica_type, comment from V\$OB_LS_REPLICA_TASK_PLAN where tenant_id = $tenant_id and ls_id = $ls_id order by priority, task_type, target_replica_type;
# trigger one round disaster worker
ob1.mysql set ob_global_debug_sync = 'now signal signal1';
# read ls replica task
ob1.check_until_timeout select (count(*) != 0) as cnt from oceanbase.CDB_OB_LS_REPLICA_TASKS where tenant_id = $tenant_id and ls_id = $ls_id, 1, 30;
eval select tenant_id, ls_id, task_type, priority, target_replica_type, comment from CDB_OB_LS_REPLICA_TASKS;
# trigger one round task execute
ob1.mysql set ob_global_debug_sync = 'now signal signal2';
# wait till task done
ob1.check_until_timeout select (count(*) = 0) as cnt from oceanbase.CDB_OB_LS_REPLICA_TASKS where tenant_id = $tenant_id and ls_id = $ls_id, 1, 200;
