# ==== Purpose ====
#
# Temporarily disable the binary log by setting @@session.sql_log_bin=0.
#
# The old value is stored in a session variable, so that it can be restored
# using restore_binlog.inc
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/disable_binlog.inc
# ... do things with binlog disabled ...
# --source mysql_test/test_suite/mysql8_t_2/include/restore_binlog.inc

--let $include_filename = disable_binlog.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--disable_query_log
SET @session.disable_binlog_save = @@session.sql_log_bin,
    @@session.sql_log_bin = 0;

--let $include_filename = disable_binlog.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
