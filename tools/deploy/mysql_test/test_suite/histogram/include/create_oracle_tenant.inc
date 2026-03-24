let $char_charset = 'utf8';

connect (sys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys;

--disable_query_log
--disable_warnings
call oceanbase.drop_tenant_force('tenant_stat_test');
--enable_warnings

eval call oceanbase.create_oracle_tenant_2c2g_with_arg('tenant_stat_test', "charset=$char_charset");

alter tenant tenant_stat_test set variables autocommit='on';
alter tenant tenant_stat_test set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant tenant_stat_test set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant tenant_stat_test set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

alter system set ob_compaction_schedule_interval = '10s' tenant=sys;
alter system set ob_compaction_schedule_interval = '10s' tenant=all_user;
alter system set ob_compaction_schedule_interval = '10s' tenant=all_meta;

--enable_query_log
