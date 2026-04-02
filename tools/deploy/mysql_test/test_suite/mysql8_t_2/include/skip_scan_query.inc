set optimizer_switch = 'skip_scan=on';
#FLUSH STATUS;;
--disable_result_log
#--eval EXPLAIN $query
--enable_result_log
--eval $query
#FLUSH STATUS;  # Depends on the query plan.
SHOW STATUS LIKE 'handler_read%';
--disable_query_log

--error 0,1051
DROP TABLE skip_scan;
--eval CREATE TABLE skip_scan $query
--enable_query_log

set optimizer_switch = 'skip_scan=off';
#FLUSH STATUS;;
--disable_result_log
#--eval EXPLAIN $query
--enable_result_log
--eval $query
#FLUSH STATUS;  # Depends on the query plan.
SHOW STATUS LIKE 'handler_read%';
--disable_query_log
--error 0,1051
DROP TABLE no_skip_scan;
--eval CREATE TABLE no_skip_scan $query
--enable_query_log

--let $diff_tables= test.skip_scan, test.no_skip_scan
--source mysql_test/test_suite/mysql8_t_2/include/diff_tables.inc

#FLUSH STATUS;;
--disable_result_log
#--eval EXPLAIN $hint_query
--enable_result_log
--eval $hint_query
#FLUSH STATUS;  # Depends on the query plan.
--disable_result_log
SHOW STATUS LIKE 'handler_read%';
--enable_result_log

--error 0,1051
DROP TABLE hint_skip_scan;

--disable_query_log
--eval CREATE TABLE hint_skip_scan $hint_query
--enable_query_log

--let $diff_tables= test.hint_skip_scan, test.no_skip_scan
--source mysql_test/test_suite/mysql8_t_2/include/diff_tables.inc

--disable_query_log
DROP TABLE skip_scan, no_skip_scan, hint_skip_scan;
--enable_query_log
