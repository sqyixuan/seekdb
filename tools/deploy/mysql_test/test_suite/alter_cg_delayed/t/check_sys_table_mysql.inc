# input: $table_name
# output: statistics in __all_column_group and __all_column_group_mapping
# notice: connection is managed outside

--disable_query_log
eval select oceanbase.__all_column_group.column_group_name, oceanbase.__all_column_group.column_group_type, oceanbase.__all_column_group_mapping.column_id from oceanbase.__all_column_group left join oceanbase.__all_column_group_mapping on oceanbase.__all_column_group.tenant_id = oceanbase.__all_column_group_mapping.tenant_id and oceanbase.__all_column_group.table_id = oceanbase.__all_column_group_mapping.table_id and oceanbase.__all_column_group.column_group_id = oceanbase.__all_column_group_mapping.column_group_id where oceanbase.__all_column_group.table_id = ( select max(table_id) from oceanbase.__all_table where table_name = $table_name ) order by oceanbase.__all_column_group.column_group_name asc, oceanbase.__all_column_group_mapping.column_id asc;
--enable_query_log
