--let $include_filename= uninstall_group_replication_plugin.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

# Uninstall group replication plugin
--disable_query_log
--disable_warnings
UNINSTALL PLUGIN group_replication;
--enable_warnings
--enable_query_log
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
