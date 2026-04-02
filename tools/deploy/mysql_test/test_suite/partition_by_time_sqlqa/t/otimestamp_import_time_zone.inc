
#--echo
#--echo ########### import time zone info ###########

connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;

--disable_query_log
--disable_result_log
--disable_warnings

select count(*) c1 from oceanbase.__all_time_zone;
let $tz_count= query_get_value(select count(*) c1 from oceanbase.__all_time_zone, c1, 1);
if ($tz_count < 100)
{
  --source time_zone_info.sql
  sleep 5;
  let $test_count=1;
  while ($test_count < 20)
  {
    --error 0, 1525
    let $result= query_get_value(select 1 as c1 from dual where Timestamp '2012-12-01 12:01:02.123 Asia/Shanghai' < systimestamp, c1, 1);
    if ($result != 1) {
      sleep 1;
      inc $test_count;
    }
  }
}

--enable_query_log
--enable_result_log
--enable_warnings

disconnect obsys;

connection default;


