#--source mysql_test/include/check_ob_or_mysql.inc
#if ($cluster_is_ob == 1)
#{
connect (obsys_daft,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_daft;
--disable_query_log
--disable_result_log
let $recyclebin_var = query_get_value(show variables like 'recyclebin', Value, 1);
set @@recyclebin = on;

let $tmp_tenant_id=query_get_value(select effective_tenant_id(), effective_tenant_id(), 1);
let $tmp_tenant_name=query_get_value(select effective_tenant(), effective_tenant(), 1);
let $tmp_user_name=query_get_value(select current_user(), current_user(), 1);
let $need_change_conn=query_get_value(select ($tmp_tenant_id != 1) or ('$tmp_user_name' not like 'root@%' and '$tmp_user_name' not like "admin@%") as need_change, need_change, 1);
if ($need_change_conn == 1)
{
  connection obsys_daft;
}
let $tmp_table_id = query_get_value(select table_id from oceanbase.__all_virtual_ddl_operation where ddl_stmt_str != '' and tenant_id = $tmp_tenant_id order by schema_version desc limit 1, table_id, 1);
if ($is_truncate_table == 1)
{
  let $tmp_table_id = query_get_value(select table_id from oceanbase.__all_virtual_ddl_operation where ddl_stmt_str != '' and tenant_id = $tmp_tenant_id order by schema_version desc limit 1, table_id, 1);
}
let $tmp_table_name = query_get_value(select table_name from oceanbase.__all_virtual_table where table_id = $tmp_table_id and tenant_id = $tmp_tenant_id, table_name, 1);
let $tmp_db_id = query_get_value(select database_id from oceanbase.__all_virtual_table where table_id = $tmp_table_id and tenant_id = $tmp_tenant_id, database_id, 1);
let $tmp_table_type = query_get_value(select table_type from oceanbase.__all_virtual_table where table_id = $tmp_table_id and tenant_id = $tmp_tenant_id, table_type, 1);
let $tmp_db_name = query_get_value(select database_name from oceanbase.__all_virtual_database where database_id = $tmp_db_id and tenant_id = $tmp_tenant_id, database_name, 1);
if ($need_change_conn == 1)
{
  let $tmp_server_ip = $OBMYSQL_MS0;
  let $tmp_server_port = $OBMYSQL_PORT;
  let $tmp_is_valid_ip = query_get_value(select '$TE_SERVER_IP' not like '' and '$TE_SERVER_IP' not like 'null' as ip, ip, 1);
  if ($tmp_is_valid_ip == 1)
  {
    let $tmp_server_ip = $TE_SERVER_IP;
    let $tmp_server_port = $TE_SERVER_PORT;
  }
  connect (tmp_connection, $tmp_server_ip, root@$tmp_tenant_name,,*NO-ONE*,$tmp_server_port);
}
--enable_result_log
if ($tmp_table_type == 4)
{
  eval drop view `$tmp_db_name`.`$tmp_table_name`;
}
if ($tmp_table_type != 4)
{
  eval drop table `$tmp_db_name`.`$tmp_table_name`;
}

--disable_result_log
#if ($need_change_conn)
#{
#  connection obsys_daft;
#}
let $tmp_obj_name = query_get_value(select object_name from oceanbase.__all_virtual_recyclebin where tenant_id = $tmp_tenant_id and table_id = $tmp_table_id, object_name, 1);
if ($need_change_conn == 1)
{
   connection tmp_connection;
}
--enable_result_log
eval flashback table $tmp_obj_name to before drop;
--disable_result_log
eval set @@recyclebin = $recyclebin_var;
if ($need_change_conn)
{
  disconnect tmp_connection;
}
disconnect obsys_daft;
connection default;
--enable_query_log
--enable_result_log
#}
