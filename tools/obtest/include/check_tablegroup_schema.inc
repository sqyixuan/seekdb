# author : zhaoziqian.zzq
# input : $tg_name

let $tg_id = deploy_get_value(ob1.select select tablegroup_id from oceanbase.__all_virtual_tablegroup where tablegroup_name = $tg_name, tablegroup_id, 1);
ob1.mysql_p select tenant_id, tablegroup_id, tablegroup_name, part_level, part_func_type, part_num, sub_part_func_type, sub_part_num, part_func_expr_num, sub_part_func_expr_num from oceanbase.__all_virtual_tablegroup where tablegroup_id = $tg_id;
ob1.mysql_p select tenant_id, table_id, part_type, part_num, sub_part_type, def_sub_part_num, part_expr, sub_part_expr from oceanbase.__all_virtual_part_info where table_id = $tg_id;
ob1.mysql_p select tenant_id, table_id, part_id, part_name, high_bound_val, sub_part_num from oceanbase.__all_virtual_part where table_id = $tg_id;
ob1.mysql_p select tenant_id, table_id, sub_part_id, sub_part_name, high_bound_val from oceanbase.__all_virtual_def_sub_part where table_id = $tg_id;
ob1.mysql_p select tenant_id, table_id, part_id, sub_part_id, sub_part_name, high_bound_val from oceanbase.__all_virtual_sub_part where table_id = $tg_id;
echo "";
