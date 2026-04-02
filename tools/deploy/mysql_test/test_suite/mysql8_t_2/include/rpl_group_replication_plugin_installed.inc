--let $include_filename= rpl_group_replication_plugin_installed.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--let $group_replication_plugin_installed=`select count(*) from information_schema.plugins where PLUGIN_NAME like 'group_replication'`

--let $include_filename= rpl_group_replication_plugin_installed.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
