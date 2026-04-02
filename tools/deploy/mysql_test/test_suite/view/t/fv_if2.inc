--disable_query_log
eval SET @my_select = 'SELECT IF($col_type IS NULL, ''IS     NULL'',
''IS NOT NULL''), $col_type, id FROM t1_values';
--source mysql_test/test_suite/view/t/fv1.inc
