connect (sys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys;

--disable_query_log
--disable_warnings
call oceanbase.drop_tenant_force('tenant_charset_test');
--enable_warnings

eval call oceanbase.create_oracle_tenant_1c2g_with_arg('tenant_charset_test', "charset=$char_charset");

alter tenant tenant_charset_test set variables autocommit='on';
alter tenant tenant_charset_test set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant tenant_charset_test set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant tenant_charset_test set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

--enable_query_log
