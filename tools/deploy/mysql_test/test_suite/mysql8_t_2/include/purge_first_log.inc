# ==== Purpose ====
#
# Purge the first (oldest) binary log.
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/purge_first_log.inc

--let $include_filename= purge_first_log.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--let $_purge_first_log= query_get_value(SHOW BINARY LOGS, Log_name, 2)
--disable_query_log
eval PURGE BINARY LOGS TO '$_purge_first_log';

--let $include_filename= purge_first_log.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
