--let $include_filename= uninstall_semisync.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--disable_query_log
--connection slave
--source mysql_test/test_suite/mysql8_t_2/include/stop_slave_io.inc

# Uninstall rpl_semi_sync_replica first
--disable_warnings
UNINSTALL PLUGIN rpl_semi_sync_replica;

--connection master
# After BUG#17638477 fix, uninstallation of rpl_semi_sync_source
#  is not allowed when there are semi sync slaves. Hence kill
#  all dump threads before uninstalling it.
SET GLOBAL rpl_semi_sync_source_enabled = OFF;
--source mysql_test/test_suite/mysql8_t_2/include/stop_dump_threads.inc
UNINSTALL PLUGIN rpl_semi_sync_source;
--enable_warnings

--connection slave
--source mysql_test/test_suite/mysql8_t_2/include/start_slave_io.inc
--enable_query_log

--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
