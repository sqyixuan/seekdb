# make sure connection obsys to tenant root@sys exists

--let $table_name = "t1"
--source mysql_test/include/row_col_switch_check_major_sstables.inc
--let $table_name = "t2"
--source mysql_test/include/row_col_switch_check_major_sstables.inc
--let $table_name = "t3"
--source mysql_test/include/row_col_switch_check_major_sstables.inc
--let $table_name = "t4"
--source mysql_test/include/row_col_switch_check_major_sstables.inc
--let $table_name = "t5"
--source mysql_test/include/row_col_switch_check_major_sstables.inc
--let $table_name = "t6"
--source mysql_test/include/row_col_switch_check_major_sstables.inc
