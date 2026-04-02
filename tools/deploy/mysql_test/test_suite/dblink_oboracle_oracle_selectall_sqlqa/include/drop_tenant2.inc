connection sys2;
--disable_query_log
--error 0,5157
drop tenant dblink_tnt2 force;
--enable_query_log
