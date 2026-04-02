# ==== Purpose ====
#
# Wait for the given server to be elected as the Primary member of the group
#
# ==== Usage ====
#
# [--let $wait_timeout= NUMBER]
# [--let $rpl_debug= 1]
# --let $group_replication_expected_uuid=UUID
# --source mysql_test/test_suite/mysql8_t_2/include/gr_wait_primary_member_uuid.inc
#

--let $include_filename= gr_wait_primary_member_uuid.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

let $wait_condition= SELECT COUNT(*) = 1 FROM performance_schema.global_status
   WHERE VARIABLE_NAME = "group_replication_primary_member" AND
         VARIABLE_VALUE = "$group_replication_expected_uuid";
--source mysql_test/test_suite/mysql8_t_2/include/wait_condition_or_abort.inc

--let $include_filename= gr_wait_primary_member_uuid.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc

--let $group_replication_expected_uuid=
--let $group_replication_member_role=
