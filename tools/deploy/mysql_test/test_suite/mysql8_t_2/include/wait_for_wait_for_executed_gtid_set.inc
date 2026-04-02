# ==== Purpose ====
#
# Wait until WAIT_FOR_EXECUTED_GTID_SET goes to the wait state.
#
# ==== Usage ====
#
# --let $thread_id= THREAD_ID
# --source mysql_test/test_suite/mysql8_t_2/include/wait_for_wait_for_executed_gtid_set.inc

--let $include_filename= wait_for_wait_for_executed_gtid_set.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc


--let $wait_condition= SELECT COUNT(*) = 1 FROM performance_schema.threads WHERE PROCESSLIST_ID = $thread_id AND PROCESSLIST_STATE = 'Waiting for GTID to be committed'
--source mysql_test/test_suite/mysql8_t_2/include/wait_condition.inc


--let $include_filename= wait_for_wait_for_executed_gtid_set.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
