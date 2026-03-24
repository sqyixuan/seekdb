# ==== Purpose ====
#
# Clean monitoring process related tasks if needed
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/clean_monitoring_process.inc
#
# Please see include/restart_with_mysqld_safe_cleanup.inc
#

--let $include_filename= clean_monitoring_process.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

let $have_windows = `SELECT CONVERT(@@version_compile_os using latin1) IN ("Win32", "Win64", "Windows")`;

if (!$have_windows) {
  --source mysql_test/test_suite/mysql8_t_2/include/restart_with_mysqld_safe_cleanup.inc
}

--let $include_filename= clean_monitoring_process.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
