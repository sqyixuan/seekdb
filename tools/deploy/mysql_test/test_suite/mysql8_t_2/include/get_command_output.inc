# ==== Purpose ====
#
# Execute a command, and capture stdout in a mysqltest variable.
#
# ==== Usage ====
#
# --let $command= COMMAND
# --source mysql_test/test_suite/mysql8_t_2/include/get_command_output.inc
# --echo output was: $result

--let $include_filename= get_command_output.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

if (!$command)
{
  --die !!!ERROR IN TEST: set $command first
}

--let $read_from_file= `SELECT UUID()`
--let $read_from_file= $MYSQLTEST_VARDIR/tmp/_command_output_$read_from_file

--exec $command > $read_from_file

--source mysql_test/test_suite/mysql8_t_2/include/read_file_to_var.inc

--let $read_from_file=

--let $include_filename= get_command_output.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
