--disable_query_log
--disable_result_log
if ($DBLINK != '') {
  connection dblink_conn;
}
--enable_result_log
--enable_query_log

