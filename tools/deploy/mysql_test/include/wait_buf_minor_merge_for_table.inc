# set __table_id__ outside

--disable_query_log
--disable_result_log

connect (obsys_minor,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_minor;

let $major_snapshot  = query_get_value(select frozen_timestamp from oceanbase.__all_virtual_freeze_info order by frozen_scn desc limit 1, frozen_timestamp, 1);

let $i=300;
while ($i > 0)
{
  sleep 1;
  dec $i;
  let $minor_count = query_get_value(select count(1) as c from oceanbase.__all_virtual_table_mgr where tablet_id in (select tablet_id from oceanbase.__all_virtual_tablet_to_ls where table_id = $__table_id__) limit 1, c, 1);
  if ($minor_count == 1)
  {
    let $i = -5;
  }
}

if ($i != -5)
{
  --echo buf minor failed buf_minor_count=$minor_count
}

disconnect obsys_minor;
connection default;

--enable_query_log
--enable_result_log
