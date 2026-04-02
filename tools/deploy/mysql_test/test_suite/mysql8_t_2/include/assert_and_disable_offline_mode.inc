# ==== Purpose ====
#
# Assert that offline_mode variable is enabled and reset its value.
#
# ==== Usage ====
#
# [--let $skip_reset_offline_mode= 1]
# [--let $rpl_debug= 1]
# --source mysql_test/test_suite/mysql8_t_2/include/assert_and_disable_offline_mode.inc
#
# Parameters:
#   $skip_reset_offline_mode
#     Skip reset offline_mode variable.
#
#   $rpl_debug
#     See include/rpl_init.inc

--let $include_filename= assert_and_disable_offline_mode.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

if (!$rpl_debug)
{
  --disable_query_log
}

--let $wait_condition= SELECT @@GLOBAL.offline_mode = 1
--source mysql_test/test_suite/mysql8_t_2/include/wait_condition_or_abort.inc

if (!$skip_reset_offline_mode)
{
  SET @@GLOBAL.offline_mode = OFF;
}
--let $skip_reset_offline_mode=

--let $include_filename= assert_and_disable_offline_mode.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
