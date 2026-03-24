# require $tenant_id

desc t1;

connection obsys;

# main table schema
--replace_regex /tenant_id = [0-9]*/tenant_id = $tenant_id/g
eval select table_name, part_func_type, part_func_expr, part_num, sub_part_func_type, sub_part_func_expr, sub_part_num from __all_virtual_table where tenant_id = $tenant_id and table_name = 't1' order by gmt_create desc limit 1;

# main table column
--replace_regex /tenant_id = [0-9]*/tenant_id = $tenant_id/g
eval select column_id, prev_column_id, column_name, partition_key_position, orig_default_value_v2, cur_default_value_v2 from __all_virtual_column where tenant_id = $tenant_id and table_id = (
    select table_id from __all_virtual_table where tenant_id = $tenant_id and table_name = 't1' order by gmt_create desc limit 1
) order by column_id;

# index table schema 
--replace_regex /tenant_id = [0-9]*/tenant_id = $tenant_id/g
eval select part_func_type, part_func_expr, part_num, sub_part_func_type, sub_part_func_expr, sub_part_num from __all_virtual_table where tenant_id = $tenant_id and data_table_id <> table_id and data_table_id = (
    select table_id from __all_virtual_table where tenant_id = $tenant_id and table_name = 't1' order by gmt_create desc limit 1
) order by table_id;

# index table column
--replace_regex /tenant_id = [0-9]*/tenant_id = $tenant_id/g
eval select column_id, prev_column_id, column_name, partition_key_position, orig_default_value, cur_default_value, orig_default_value_v2, cur_default_value_v2 from __all_virtual_column where tenant_id = $tenant_id and table_id in
(select table_id from __all_virtual_table where tenant_id = $tenant_id and data_table_id <> table_id and data_table_id = (
    select table_id from __all_virtual_table where tenant_id = $tenant_id and table_name = 't1' order by gmt_create desc limit 1
)) order by table_id, column_id;

# constraint info
--replace_regex /tenant_id = [0-9]*/tenant_id = $tenant_id/g
eval select check_expr, constraint_type, rely_flag, enable_flag, validate_flag from __all_virtual_constraint where tenant_id = $tenant_id and table_id = (
    select table_id from __all_virtual_table where tenant_id = $tenant_id and table_name = 't1' order by gmt_create desc limit 1
);

connection default;
