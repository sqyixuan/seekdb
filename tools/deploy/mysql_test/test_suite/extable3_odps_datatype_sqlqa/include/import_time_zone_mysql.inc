
#--echo
#--echo ########### import time zone info ###########

--disable_query_log
--disable_result_log
--disable_warnings

--source mysql_test/include/check_schema_sync.inc
connect (conadmin,$OBMYSQL_MS0,admin@sys,admin,,$OBMYSQL_PORT);
connection conadmin;
let $ADMIN_SVR_IP = query_get_value('select SVR_IP from oceanbase.DBA_OB_SERVERS',SVR_IP,1);
let $ADMIN_SQL_PORT = query_get_value('select SQL_PORT from oceanbase.DBA_OB_SERVERS',SQL_PORT,1);

connect (conn_admin_tz,$ADMIN_SVR_IP,root,,test,$ADMIN_SQL_PORT);
connection conn_admin_tz;
alter system change tenant sys;

connect (conn_mysql,$OBMYSQL_MS0,root,,*NO-ONE*,$OBMYSQL_PORT);
connection conn_mysql;

--error 0, 1298
set time_zone = 'Asia/Shanghai';
let $result= query_get_value(select @@time_zone as c1 from dual, c1, 1);
if ($result != 'Asia/Shanghai')
{
  connection conn_admin_tz;
  --source mysql_test/test_suite/time_zone/t/time_zone_info.sql
  --disable_query_log
  --disable_result_log
  sleep 5;
  let $test_count=1;
  connection conn_mysql;
  while ($test_count < 30)
  {
    --error 0, 1298
    set time_zone = 'Asia/Shanghai';
    let $result= query_get_value(select @@time_zone as c1 from dual, c1, 1);
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
disconnect conn_mysql;
disconnect conadmin;
connection default;

--enable_result_log
--enable_warnings
