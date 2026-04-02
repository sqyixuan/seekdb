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

connection oracle;
set @@session.recyclebin = off;
eval drop table non_partitioned_tb_$postfix;
eval drop table hash_tb_$postfix;
eval drop table range_hash_tb_$postfix;

eval drop view user_view_$postfix;

eval drop package package_$postfix;
eval drop type type_$postfix;
eval drop procedure procedure_$postfix;
eval drop function function_$postfix;
eval drop trigger tr_$postfix;

eval drop sequence seq_$postfix;
eval drop synonym sys.spn_$postfix;
eval drop context context_$postfix;
eval drop user db_$postfix cascade;
eval drop user user_$postfix cascade;

eval drop tablegroup non_partitioned_tg_$postfix;
eval drop tablegroup hash_tg_$postfix;
eval drop tablegroup range_columns_hash_tg_$postfix;

--enable_query_log
--enable_warnings
--enable_result_log
--enable_abort_on_error
