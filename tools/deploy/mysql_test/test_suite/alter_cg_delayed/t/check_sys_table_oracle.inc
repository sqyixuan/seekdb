# input: $table_name, $tenant_id
# output: statistics in __all_column_group and __all_column_group_mapping
# notice: connection is managed inside

connect (inner_sys,$OBMYSQL_MS0,root@sys,,Oceanbase,$OBMYSQL_PORT);
connection inner_sys;
--disable_query_log
eval select __all_virtual_column_group.column_group_name, __all_virtual_column_group.column_group_type, __all_virtual_column.column_name from __all_virtual_column_group left join ( __all_virtual_column_group_mapping left join __all_virtual_column on __all_virtual_column.table_id = __all_virtual_column_group_mapping.table_id and __all_virtual_column.column_id = __all_virtual_column_group_mapping.column_id and __all_virtual_column.tenant_id = __all_virtual_column_group_mapping.tenant_id ) on __all_virtual_column_group.column_group_id = __all_virtual_column_group_mapping.column_group_id and __all_virtual_column_group.table_id = __all_virtual_column_group_mapping.table_id and __all_virtual_column_group.tenant_id = __all_virtual_column_group_mapping.tenant_id where __all_virtual_column_group.table_id = ( select max(table_id) from __all_virtual_table where table_name = $table_name and tenant_id = $tenant_id ) and __all_virtual_column_group.tenant_id = $tenant_id order by __all_virtual_column_group.column_group_name, __all_virtual_column.column_name;
--enable_query_log
disconnect inner_sys;
connection default;
