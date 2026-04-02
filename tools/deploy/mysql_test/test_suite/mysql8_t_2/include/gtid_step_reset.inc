# ==== Purpose ====
#
# Remember the current value of @@GLOBAL.GTID_EXECUTED to be used by
# subsequent calls to gtid_step_assert.inc
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/gtid_step_reset.inc
#
# See gtid_step_assert.inc for details and examples.

--let $include_filename= gtid_step_reset.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--let $gtid_step_last= `SELECT @@GLOBAL.GTID_EXECUTED`

--let $include_filename= gtid_step_reset.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
