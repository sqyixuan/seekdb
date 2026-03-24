
let $sql = select t.table_name, p.part_type, p.part_num, p.new_part_num, p.sub_part_type, p.def_sub_part_num, p.part_expr, p.sub_part_expr from oceanbase . __all_virtual_table as t inner join oceanbase . __all_virtual_part_info as p on p.table_id = t.table_id where t.table_name = $table_name and table_schema = $table_schema;
eval $sql;

let $sql = select t.table_name, p.part_name, p.sub_part_num, p.new_sub_part_num, p.def_compress_method from oceanbase . __all_virtual_table as t inner join oceanbase . __all_virtual_part as p on p.table_id = t.table_id where t.table_name = $table_name and table_schema = $table_schema;
eval $sql;

let $sql = select t.table_name, p.sub_part_name from oceanbase . __all_virtual_table as t inner join oceanbase . __all_virtual_sub_part as p on p.table_id = t.table_id where t.table_name = $table_name and table_schema = $table_schema;
eval $sql;

