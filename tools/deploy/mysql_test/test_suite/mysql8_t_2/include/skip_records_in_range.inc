SET optimizer_trace="enabled=on";
--sorted_result
eval $query;
SELECT TRACE INTO @trace FROM INFORMATION_SCHEMA.OPTIMIZER_TRACE;
--error 3995
SELECT @trace RLIKE "skipped_due_to_force_index";
SET optimizer_trace="enabled=off";

let $format=traditional;
--source mysql_test/test_suite/mysql8_t_2/include/explain_other.inc

let $format=json;
--source mysql_test/test_suite/mysql8_t_2/include/explain_other.inc
