let $recycle_object_name = deploy_get_value(ob1.select select object_name from oceanbase.__all_virtual_recyclebin where original_name = "BASE_TABLE_TO_FLASHBACK", object_name, 1);
eval FLASHBACK TABLE $recycle_object_name TO BEFORE DROP;

let $recycle_object_name = deploy_get_value(ob1.select select object_name from oceanbase.__all_virtual_recyclebin where original_name = "BASE_TABLE_TO_TRUNCATE", object_name, 1);
eval FLASHBACK TABLE $recycle_object_name TO BEFORE DROP RENAME TO base_table_flashbacked;

let $recycle_object_name = deploy_get_value(ob1.select select object_name from oceanbase.__all_virtual_recyclebin where original_name = "BASE_VIEW_TO_FLASHBACK", object_name, 1);
eval FLASHBACK TABLE $recycle_object_name TO BEFORE DROP;
