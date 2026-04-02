--disable_query_log
--disable_result_log

connect (root,$OBMYSQL_MS0,root,,test,$OBMYSQL_PORT);
connection root;
alter system set _ob_enable_direct_load=false;
disconnect root;
connection default;

--enable_query_log
--enable_result_log
