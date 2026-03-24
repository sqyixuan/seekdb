# ==== Purpose ====
#
# Read the contents of a file and store it in a mysqltest variable.
#
# ==== Usage ====
#
# --let $read_from_file= <FILENAME>
# --source mysql_test/test_suite/mysql8_t_2/include/read_file_to_var.inc
# --echo $result
#
# Parameters:
#
#   $read_from_file
#     The file to read from.
#
# Return value:
#   The result is stored in the mysqltest variable $result.

--let $include_filename= read_file_to_var.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--let $_rftv_secure_file_priv= `SELECT @@secure_file_priv`
if ($_rftv_secure_file_priv)
{
  --let $_rftv_suffix= `SELECT UUID()`
  --let $_rftv_tmp_file= $_rftv_secure_file_priv/_rftv_$_rftv_suffix

  --copy_file $read_from_file $_rftv_tmp_file
  --let $result= `SELECT TRIM(TRAILING '\n' FROM LOAD_FILE('$_rftv_tmp_file'))`
  --remove_file $_rftv_tmp_file
}
if (!$_rftv_secure_file_priv)
{
  --let $result= `SELECT TRIM(TRAILING '\n' FROM LOAD_FILE('$read_from_file'))`
}

if ($rpl_debug)
{
  --echo debug: result=<<<$result>>>
}

--let $include_filename= read_file_to_var.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
