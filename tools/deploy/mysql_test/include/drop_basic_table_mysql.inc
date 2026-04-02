--disable_query_log
--disable_warnings
--disable_result_log
--disable_abort_on_error

connection sys;
use test;
set @@session.recyclebin = off;
eval drop table if exists non_partitioned_tb_$postfix;
eval drop table if exists key_tb_$postfix;
eval drop table if exists range_columns_tb_$postfix;
eval drop table if exists list_columns_tb_$postfix;
eval drop table if exists key_list_columns_tb_$postfix;
eval drop table if exists range_columns_key_tb_$postfix;
eval drop table if exists list_columns_range_columns_tb_$postfix;
eval drop table if exists spatial_col_tb_$postfix;
eval drop table if exists mysql_date_tb_$postfix;
eval drop table if exists mysql_datetime_tb_$postfix;

connection mysql;
use test;
set @@session.recyclebin = off;
eval drop table if exists non_partitioned_tb_$postfix;
eval drop table if exists key_tb_$postfix;
eval drop table if exists range_columns_tb_$postfix;
eval drop table if exists list_columns_tb_$postfix;
eval drop table if exists key_list_columns_tb_$postfix;
eval drop table if exists range_columns_key_tb_$postfix;
eval drop table if exists list_columns_range_columns_tb_$postfix;
eval drop table if exists spatial_col_tb_$postfix;
eval drop table if exists mysql_date_tb_$postfix;
eval drop table if exists mysql_datetime_tb_$postfix;

--enable_query_log
--enable_warnings
--enable_result_log
--enable_abort_on_error
