# Setup Replication Observers Example plugin
# Usage:
# --source mysql_test/test_suite/mysql8_t_2/include/install_replication_observers_example.inc

--let $include_filename= install_replication_observers_example.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--source mysql_test/test_suite/mysql8_t_2/include/have_replication_observers_example_plugin.inc

--disable_query_log

--let $value= query_get_value("SELECT Plugin_Name FROM information_schema.plugins WHERE Plugin_Name LIKE 'replication_observers_example'", Plugin_Name, 1)
if ($value == No such row)
{
  eval INSTALL PLUGIN replication_observers_example SONAME '$RPL_OBS_EXAMPLE';
}

--enable_query_log

--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
