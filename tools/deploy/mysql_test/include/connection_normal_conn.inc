--disable_query_log
--disable_result_log
connection default;
if ($DBLINK != '') {
  connection normal_conn;
}
--enable_result_log
--enable_query_log

