# ==== Purpose ====
#
# Removes the functions created by sourcing gr_autorejoin_monitoring.inc and
# disables thread stage events for the auto-rejoin process.
#
# Should be sourced at the end of a test that also sourced
# gr_autorejoin_monitoring.inc
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/gr_end_autorejoin_monitoring.inc
#
--let $include_filename = gr_end_autorejoin_monitoring.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

--disable_query_log
SET SESSION sql_log_bin = 0;

DROP FUNCTION IS_AUTOREJOIN_RUNNING;
DROP FUNCTION GET_NUMBER_RETRIES;
DROP FUNCTION GET_TIME_UNTIL_NEXT_RETRY;
DROP FUNCTION GET_LAST_AUTOREJOIN;
DROP FUNCTION GET_COUNT_AUTOREJOIN;

SET SESSION sql_log_bin = 1;
--enable_query_log

--let $include_filename = gr_end_autorejoin_monitoring.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
