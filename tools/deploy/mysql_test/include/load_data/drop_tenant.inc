connection sys;
--disable_query_log
--disable_warnings
eval DROP TENANT IF EXISTS $TENANT_NAME FORCE;
eval DROP RESOURCE POOL IF EXISTS pool_$TENANT_NAME;
eval DROP RESOURCE UNIT IF EXISTS uint_$TENANT_NAME;
--enable_warnings
--enable_query_log
