
#--echo
#--echo ########### import time zone info ###########

--disable_query_log
--disable_result_log
--disable_warnings

connect (conn_admin_tz, $OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin_tz;
eval alter system change tenant $TENANT;
sleep 1;

connect (conn1,$OBMYSQL_MS0,root@$TENANT,,*NO-ONE*,$OBMYSQL_PORT);
connection conn1;
--error 0, 1298
set time_zone = 'Asia/Shanghai';
let $result= query_get_value(select @@time_zone as c1 from dual, c1, 1);
if ($result != 'Asia/Shanghai')
{
  connection conn_admin_tz;
  if ($tz_file == 0) {
    --source mysql_test/test_suite/time_zone/t/time_zone_info.sql
  }
  if ($tz_file == 1) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_more_tz.sql
  }
  if ($tz_file == 2) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_less_tz.sql
  }
  if ($tz_file == 3) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_diff_tz_name.sql
  }
  if ($tz_file == 4) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_diff_order.sql
  }
  if ($tz_file == 5) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_more_trans.sql
  }
  if ($tz_file == 6) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_less_trans.sql
  }
  if ($tz_file == 7) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_diff_trans_name.sql
  }
  if ($tz_file == 8) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_diff_trans_time.sql
  }
  if ($tz_file == 9) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_diff_trans_type.sql
  }
  if ($tz_file == 10) {
    --source mysql_test/test_suite/time_zone/t/include/time_zone_info_diff_trans_offset.sql
  }
  --disable_query_log
  --disable_result_log
  sleep 5;
  let $test_count=1;
  connection conn1;
  while ($test_count < 30)
  {
    --error 0, 1298
    set time_zone = 'Asia/Shanghai';
    let $result= query_get_value(select @@time_zone as c2 from dual, c2, 1);
    eval select $test_count as test_count, '$result' as result from dual;
    if ($result != 'Asia/Shanghai') {
      sleep 1;
      inc $test_count;
    }
    if ($result == 'Asia/Shanghai') {
      let $test_count = 30;
    }
  }
}

--enable_query_log
disconnect conn_admin_tz;
disconnect conn1;
connection default;

--enable_result_log
--enable_warnings
