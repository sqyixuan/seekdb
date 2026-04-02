# include/wait_until_rows_count.inc
#
# SUMMARY
#
#    Waits until SELECT count(*) = $count from $table returns true, or
#    the operation times out.
#
# USAGE
#
#    let $count= 5;
#    let $table= t1;
#    --source mysql_test/test_suite/mysql8_t_2/include/wait_until_rows_count.inc
#

let $wait_condition=
  select count(*) = $count from $table;
--source mysql_test/test_suite/mysql8_t_2/include/wait_condition.inc
