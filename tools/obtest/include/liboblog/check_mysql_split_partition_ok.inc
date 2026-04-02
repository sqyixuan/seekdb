--disable_query_log
--disable_result_log

ob_connect(conn_sys, ob1.proxy0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);

sleep 3;
alter system major freeze;
--echo major_freeze
let $i=150;
while($i>0)
{
  sleep 3;
  let $result1 = query_get_value(select count(*) as count from oceanbase.__all_virtual_table where partition_status !=0, count, 1);
  let $result = query_get_value(select count(*) as count from oceanbase.__all_virtual_tablegroup where partition_status !=0, count, 1);
  if($result == 0)
  {
    if ($result1 == 0)
    {
      let $i =0;
    }
  }

  dec $i;
}
if($i != -1)
{
    --echo check split partition failed
}
connect (oblog_conn_tt,$ip,root@oblog_tt,,oblog_db,$port);
connection oblog_conn_tt;
--enable_query_log
--enable_result_log
