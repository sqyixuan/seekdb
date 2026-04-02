connection conn_admin;
--disable_warnings
--disable_query_log
--disable_result_log
drop tenant yts_tt force;
drop resource pool yts_pool;
drop resource unit yts_box;
--enable_warnings
--enable_query_log
--enable_result_log
