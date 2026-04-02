
#--echo
#--echo ########### import time zone info ###########

--disable_query_log
--disable_result_log
--disable_warnings

connect (conn_admin_tz, $OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin_tz;
alter system change tenant oracle;

connect(conn_oracle,$OBMYSQL_MS0,sys@oracle,,,$OBMYSQL_PORT);
connection conn_oracle;

--error 0, 1882
set time_zone = 'Asia/Shanghai';
let $result= query_get_value(select @@time_zone as C1 from dual, C1, 1);
if ($result != 'Asia/Shanghai')
{
  connection conn_admin_tz;
  --source mysql_test/test_suite/time_zone/t/time_zone_info.sql
  --disable_query_log
  --disable_result_log
  sleep 5;
  let $test_count=1;
  connection conn_oracle;
  while ($test_count < 30)
  {
    --error 0, 1882
    set time_zone = 'Asia/Shanghai';
    let $result= query_get_value(select @@time_zone as C1 from dual, C1, 1);
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
disconnect conn_oracle;
connection default;

--enable_result_log
--enable_warnings
