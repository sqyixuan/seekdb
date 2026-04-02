--disable_query_log
eval SET @my_select = 'SELECT CAST($col_type AS $target_type),
$col_type, id FROM t1_values';
--source mysql_test/test_suite/view/t/fv1.inc
