# set __table_name__ outside

--disable_query_log
--disable_result_log

connect (obsys_minor,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_minor;

eval alter system major freeze tenant sys;
let $__now_time__ = query_get_value(select now() as c, c, 1);
let $__tenant_id__ = query_get_value(select tenant_id from oceanbase.__all_tenant where tenant_name = 'sys', tenant_id, 1);
let $__i__= 600;
while($__i__ > 0)
{
  sleep 1;
  dec $__i__;
  let $__finish_major__= query_get_value(select count(*) as cnt from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $__tenant_id__ and last_finish_time > '$__now_time__', cnt, 1);
  if ($__finish_major__ == 1)
  {
    let $__i__ = -5;
  }
}
if ($__i__ != -5)
{
  --echo major freeze failed
}

let $__table_id__ = query_get_value(select table_id from oceanbase.__all_virtual_table where table_name=$__table_name__, table_id, 1);
let $__lob_meta_table_id__ = query_get_value(select table_id from oceanbase.__all_virtual_table where data_table_id=$__table_id__ and table_name like '%AUX_LOB_META%', table_id, 1);
let $__tablet_id__ = query_get_value(select tablet_id from oceanbase.CDB_OB_TABLE_LOCATIONS where table_id=$__lob_meta_table_id__ and ROLE = 'LEADER', tablet_id, 1);
let $__svr_ip__ = query_get_value(select svr_ip from oceanbase.CDB_OB_TABLE_LOCATIONS where table_id=$__lob_meta_table_id__ and ROLE = 'LEADER', svr_ip, 1);
let $__svr_port__ = query_get_value(select svr_port from oceanbase.CDB_OB_TABLE_LOCATIONS where table_id=$__lob_meta_table_id__ and ROLE = 'LEADER', svr_port, 1);
let $__end_log_scn__ = query_get_value(select max(end_log_scn) as log_scn from oceanbase.__all_virtual_tablet_sstable_macro_info where tablet_id = $__tablet_id__ and macro_block_type='data_block' and row_store_type = 'encoding_row_store', log_scn, 1);
let $__lob_meta_line_count__ = query_get_value(select sum(row_count) as counts from oceanbase.__all_virtual_tablet_sstable_macro_info where svr_ip = '$__svr_ip__' and svr_port = $__svr_port__ and tablet_id = $__tablet_id__ and macro_block_type='data_block' and row_store_type = 'encoding_row_store' and end_log_scn=$__end_log_scn__, counts, 1);
--echo $__lob_meta_line_count__

disconnect obsys_minor;
connection default;

--enable_query_log
--enable_result_log
