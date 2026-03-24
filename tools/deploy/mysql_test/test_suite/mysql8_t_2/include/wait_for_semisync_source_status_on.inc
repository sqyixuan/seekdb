# ==== Purpose ====
#
# Wait until rpl_semi_sync_source_status equals to ON
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/wait_for_semisync_source_status_on.inc
#
--let $status_var= rpl_semi_sync_source_status
--let $status_var_value= ON
--source mysql_test/test_suite/mysql8_t_2/include/wait_for_status_var.inc
