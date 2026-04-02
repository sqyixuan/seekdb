--disable_query_log
--source mysql_test/test_suite/compatibility_control/include/define_current_version.inc
eval set global ob_compatibility_version = $current_version;
--enable_query_log
