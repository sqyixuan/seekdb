--disable_query_log
eval SET @my_select = 'SELECT IFNULL($col_type,''IS_NULL''),
$col_type, id FROM t1_values';
--source mysql_test/test_suite/view/t/fv1.inc
