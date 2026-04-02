# ==== Purpose ====
#
# Show the tail of error log generated since the last invocation of
# save_error_log_position.inc
#
# By default, this script invokes save_error_log_position.inc at the
# end, so you can easily use it with a sequence of error-generating
# statements, like:
#
#   --source mysql_test/test_suite/mysql8_t_2/include/save_error_log_position.inc
#   <generate some error>
#   --source mysql_test/test_suite/mysql8_t_2/include/show_error_log.inc
#   <generate some error>
#   --source mysql_test/test_suite/mysql8_t_2/include/show_error_log.inc
#   ...
#
# ==== Usage ====
#
# [--let $error_log_position = TIMESTAMP]
# [--let $error_log_columns = COLUMNS]
# [--let $save_error_log_position = 0]
# --source mysql_test/test_suite/mysql8_t_2/include/show_error_log.inc
#

--let $include_filename = show_error_log.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--let $error_pattern = SHOW
--source mysql_test/test_suite/mysql8_t_2/include/assert_error_log.inc

--let $include_filename = show_error_log.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
