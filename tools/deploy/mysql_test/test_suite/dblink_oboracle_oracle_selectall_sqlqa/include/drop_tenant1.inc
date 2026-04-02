connection sys1;
--disable_query_log
--error 0,5157
drop tenant dblink_tnt1 force;
--enable_query_log
