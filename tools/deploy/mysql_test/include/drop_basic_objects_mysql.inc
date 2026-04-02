--disable_query_log
--disable_warnings
--disable_result_log
--disable_abort_on_error

connection sys;
set @@session.recyclebin = off;
use test;
eval drop table if exists non_partitioned_tb_$postfix;
eval drop table if exists hash_tb_$postfix;
eval drop table if exists range_hash_tb_$postfix;

eval drop view if exists user_view_$postfix;

eval drop procedure if exists procedure_$postfix;
eval drop function if exists function_$postfix;
eval drop trigger if exists tr_$postfix;

eval drop database if exists db_$postfix;

eval drop tablegroup if exists non_partitioned_tg_$postfix;
eval drop tablegroup if exists hash_tg_$postfix;
eval drop tablegroup if exists range_hash_tg_$postfix;

connection mysql;
set @@session.recyclebin = off;
use test;
eval drop table if exists non_partitioned_tb_$postfix;
eval drop table if exists hash_tb_$postfix;
eval drop table if exists range_hash_tb_$postfix;

eval drop view if exists user_view_$postfix;

eval drop procedure if exists procedure_$postfix;
eval drop function if exists function_$postfix;
eval drop trigger if exists tr_$postfix;

eval drop database if exists db_$postfix;

eval drop tablegroup if exists non_partitioned_tg_$postfix;
eval drop tablegroup if exists hash_tg_$postfix;
eval drop tablegroup if exists range_hash_tg_$postfix;

--enable_query_log
--enable_warnings
--enable_result_log
--enable_abort_on_error
