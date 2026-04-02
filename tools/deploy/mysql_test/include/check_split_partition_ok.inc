--disable_query_log
--disable_result_log

connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;
sleep 3;
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
disconnect obsys;
connection default;
--enable_query_log
--enable_result_log
