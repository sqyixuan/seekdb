--disable_query_log
--disable_warnings
--disable_result_log
--disable_abort_on_error
eval drop table sys.non_partitioned_tb_$postfix;
eval drop tablegroup non_partitioned_tg_$postfix;
eval drop tablegroup key_tg_$postfix;
eval drop tablegroup hash_tg_$postfix;
eval drop tablegroup range_tg_$postfix;
eval drop tablegroup range_columns_tg_$postfix;
eval drop tablegroup list_$postfix;
eval drop tablegroup list_columns_$postfix;
eval drop tablegroup key_key_tg_$postfix;
eval drop tablegroup key_hash_tg_$postfix;
eval drop tablegroup key_range_tg_$postfix;
eval drop tablegroup key_range_columns_tg_$postfix;
eval drop tablegroup key_list_tg_$postfix;
eval drop tablegroup key_list_columns_tg_$postfix;
eval drop tablegroup hash_key_tg_$postfix;
eval drop tablegroup hash_hash_tg_$postfix;
eval drop tablegroup hash_range_tg_$postfix;
eval drop tablegroup hash_range_columns_tg_$postfix;
eval drop tablegroup hash_list_tg_$postfix;
eval drop tablegroup hash_list_columns_tg_$postfix;
eval drop tablegroup range_key_tg_$postfix;
eval drop tablegroup range_hash_tg_$postfix;
eval drop tablegroup range_range_tg_$postfix;
eval drop tablegroup range_range_columns_tg_$postfix;
eval drop tablegroup range_list_tg_$postfix;
eval drop tablegroup range_list_columns_tg_$postfix;
eval drop tablegroup range_columns_key_tg_$postfix;
eval drop tablegroup range_columns_hash_tg_$postfix;
eval drop tablegroup range_columns_range_tg_$postfix;
eval drop tablegroup range_columns_range_columns_tg_$postfix;
eval drop tablegroup range_columns_list_tg_$postfix;
eval drop tablegroup range_columns_list_columns_tg_$postfix;
eval drop tablegroup list_key_tg_$postfix;
eval drop tablegroup list_hash_tg_$postfix;
eval drop tablegroup list_range_tg_$postfix;
eval drop tablegroup list_range_columns_tg_$postfix;
eval drop tablegroup list_list_tg_$postfix;
eval drop tablegroup list_list_columns_tg_$postfix;
eval drop tablegroup list_columns_key_tg_$postfix;
eval drop tablegroup list_columns_hash_tg_$postfix;
eval drop tablegroup list_columns_range_tg_$postfix;
eval drop tablegroup list_columns_range_columns_tg_$postfix;
eval drop tablegroup list_columns_list_tg_$postfix;
eval drop tablegroup list_columns_list_columns_tg_$postfix;
--enable_query_log
--enable_warnings
--enable_result_log
--enable_abort_on_error
