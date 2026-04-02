# Uninstall semisync slave plugin
# Usage:
# --source mysql_test/test_suite/mysql8_t_2/include/uninstall_semisync_replica.inc

--let $include_filename= uninstall_semisync_replica.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--disable_query_log

--source mysql_test/test_suite/mysql8_t_2/include/stop_slave_io.inc

--disable_warnings
UNINSTALL PLUGIN rpl_semi_sync_replica;
--enable_warnings

--source mysql_test/test_suite/mysql8_t_2/include/start_slave_io.inc

--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
