--error 0,1051
eval DROP TABLE $table_name;
eval CREATE TABLE $table_name (y YEAR $nullness);
eval INSERT INTO $table_name VALUES (0), (1901), (2018), (2155);
#eval ANALYZE TABLE $table_name;
eval SELECT * FROM $table_name;

let $constant=$zero;
let $colname=y;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$zero_minus_delta;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$zero_plus_delta;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$in_hole;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$min_minus_delta;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$min;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$min_plus_delta;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$inside;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$max_minus_delta;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$max;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc
let $constant=$max_plus_delta;
--source mysql_test/test_suite/mysql8_t_2/include/const_folding_cmp_ops.inc

eval DROP TABLE $table_name;
