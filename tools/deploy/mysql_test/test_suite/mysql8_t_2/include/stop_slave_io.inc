# ==== Purpose ====
#
# Issues STOP REPLICA IO_THREAD on the current connection. Then waits
# until the IO thread has stopped, or until a timeout is reached.
#
# This will fail if the slave IO thread has an error. If you expect an
# error in the IO thread, use include/wait_for_slave_io_error.inc
# instead.
#
# Please use this instead of 'STOP REPLICA IO_THREAD', to reduce the
# risk of races in test cases.
#
#
# ==== Usage ====
#
# [--let $rpl_channel_name= 'NAME']
# [--let $slave_timeout= NUMBER]
# [--let $rpl_debug= 1]
# --source mysql_test/test_suite/mysql8_t_2/include/stop_slave_io.inc
#
# Parameters:
#   $rpl_channel_name
#     If set, a FOR CHANNEL $rpl_channel_name is added.
#
#   $slave_timeout
#     See include/wait_for_slave_param.inc
#
#   $rpl_debug
#     See include/rpl_init.inc


--let $include_filename= stop_slave_io.inc
--let $_for_channel_clause=
if ($rpl_channel_name)
{
  --let $_for_channel_clause= FOR CHANNEL $rpl_channel_name
  --let $include_filename= $include_filename [FOR CHANNEL $rpl_channel_name]
}
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc


if (!$rpl_debug)
{
  --disable_query_log
}


--eval STOP REPLICA IO_THREAD $_for_channel_clause


--source mysql_test/test_suite/mysql8_t_2/include/wait_for_slave_io_to_stop.inc


--let $include_filename= stop_slave_io.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
