--disable_query_log
--disable_warnings
--disable_result_log
--disable_abort_on_error
eval drop tablegroup if exists non_partitioned_tg_$postfix;
eval drop tablegroup if exists key_tg_$postfix;
eval drop tablegroup if exists hash_tg_$postfix;
eval drop tablegroup if exists range_tg_$postfix;
eval drop tablegroup if exists range_columns_tg_$postfix;
eval drop tablegroup if exists list_$postfix;
eval drop tablegroup if exists list_columns_$postfix;
eval drop tablegroup if exists key_key_tg_$postfix;
eval drop tablegroup if exists key_hash_tg_$postfix;
eval drop tablegroup if exists key_range_tg_$postfix;
eval drop tablegroup if exists key_range_columns_tg_$postfix;
eval drop tablegroup if exists key_list_tg_$postfix;
eval drop tablegroup if exists key_list_columns_tg_$postfix;
eval drop tablegroup if exists hash_key_tg_$postfix;
eval drop tablegroup if exists hash_hash_tg_$postfix;
eval drop tablegroup if exists hash_range_tg_$postfix;
eval drop tablegroup if exists hash_range_columns_tg_$postfix;
eval drop tablegroup if exists hash_list_tg_$postfix;
eval drop tablegroup if exists hash_list_columns_tg_$postfix;
eval drop tablegroup if exists range_key_tg_$postfix;
eval drop tablegroup if exists range_hash_tg_$postfix;
eval drop tablegroup if exists range_range_tg_$postfix;
eval drop tablegroup if exists range_range_columns_tg_$postfix;
eval drop tablegroup if exists range_list_tg_$postfix;
eval drop tablegroup if exists range_list_columns_tg_$postfix;
eval drop tablegroup if exists range_columns_key_tg_$postfix;
eval drop tablegroup if exists range_columns_hash_tg_$postfix;
eval drop tablegroup if exists range_columns_range_tg_$postfix;
eval drop tablegroup if exists range_columns_range_columns_tg_$postfix;
eval drop tablegroup if exists range_columns_list_tg_$postfix;
eval drop tablegroup if exists range_columns_list_columns_tg_$postfix;
eval drop tablegroup if exists list_key_tg_$postfix;
eval drop tablegroup if exists list_hash_tg_$postfix;
eval drop tablegroup if exists list_range_tg_$postfix;
eval drop tablegroup if exists list_range_columns_tg_$postfix;
eval drop tablegroup if exists list_list_tg_$postfix;
eval drop tablegroup if exists list_list_columns_tg_$postfix;
eval drop tablegroup if exists list_columns_key_tg_$postfix;
eval drop tablegroup if exists list_columns_hash_tg_$postfix;
eval drop tablegroup if exists list_columns_range_tg_$postfix;
eval drop tablegroup if exists list_columns_range_columns_tg_$postfix;
eval drop tablegroup if exists list_columns_list_tg_$postfix;
eval drop tablegroup if exists list_columns_list_columns_tg_$postfix;
--enable_query_log
--enable_warnings
--enable_result_log
--enable_abort_on_error
