# ==== Purpose ====
#
# This waits for the file open_count to reach
# zero for the file name provided as a parameter.
#
# ==== Usage ====
#
# --let $wait_file_name=XXX
# --source mysql_test/test_suite/mysql8_t_2/include/wait_for_file_closed.inc
#

--let $include_filename= wait_for_file_closed.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--let $wait_condition= SELECT OPEN_COUNT=0 FROM performance_schema.file_instances WHERE FILE_NAME LIKE "%$wait_file_name"
--source mysql_test/test_suite/mysql8_t_2/include/wait_condition.inc

--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
