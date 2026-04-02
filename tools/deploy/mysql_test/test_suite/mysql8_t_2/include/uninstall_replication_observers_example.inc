# Uninstall Replication Observers Example plugin
# Usage:
# --source mysql_test/test_suite/mysql8_t_2/include/uninstall_replication_observers_example.inc

--let $include_filename= uninstall_replication_observers_example.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--disable_query_log

--disable_warnings
UNINSTALL PLUGIN replication_observers_example;
--enable_warnings

--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
