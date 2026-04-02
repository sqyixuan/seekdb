# Uninstall semisync master plugin
# Usage:
# --source mysql_test/test_suite/mysql8_t_2/include/uninstall_semisync_source.inc

--let $include_filename= uninstall_semisync_source.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--disable_query_log
--disable_warnings

# wait till semisync clients are '0' before uninstalling
# semisync master plugin.
--let $status_var= Rpl_semi_sync_source_clients
--let $status_var_value= 0
--source mysql_test/test_suite/mysql8_t_2/include/wait_for_status_var.inc

UNINSTALL PLUGIN rpl_semi_sync_source;

--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
