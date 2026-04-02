
#--echo
#--echo ########### import srs data ###########

--disable_query_log
--disable_result_log
--disable_warnings

connect (conn_admin, $OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection conn_admin;
alter system change tenant oracle;
sleep 2;
let $c1 = query_get_value(select count(*) as cnt from oceanbase.__all_spatial_reference_systems, cnt, 1);

if ($c1 != 5160)
{
  --source mysql_test/test_suite/geometry/t/default_srs_data_mysql.sql
  sleep 10;
}

disconnect conn_admin;
connection default;

--enable_query_log
--enable_result_log
--enable_warnings
