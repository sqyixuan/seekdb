let $cpu = 1;
let $memory = '2G';
let $disk = '1G';
let $iops = 128;
let $session = 64;
let $uint_name = uint_charset_test;
let $pool_name = pool_charset_test;
let $tenant_name = tenant_charset_test;

### connect (sys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection sys;

#let $zone = query_get_value(select zone from oceanbase.__all_virtual_zone_stat, zone, 1);

--disable_query_log
--disable_warnings
DROP TENANT IF EXISTS tenant_charset_test FORCE;
DROP RESOURCE POOL IF EXISTS pool_charset_test;
DROP RESOURCE UNIT IF EXISTS uint_charset_test;
--enable_warnings
#清空回收站
PURGE RECYCLEBIN;

eval CREATE RESOURCE UNIT $uint_name MAX_CPU=$cpu, MEMORY_SIZE=$memory;
eval CREATE RESOURCE POOL $pool_name UNIT='$uint_name', UNIT_NUM=1;
eval CREATE TENANT $tenant_name replica_num=1, charset=$char_charset, zone_list=('zone1','zone2'), primary_zone='zone1', resource_pool_list=('$pool_name');

alter tenant tenant_charset_test set variables ob_tcp_invited_nodes='%';
alter tenant tenant_charset_test set variables autocommit='on';
alter tenant tenant_charset_test set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant tenant_charset_test set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant tenant_charset_test set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';

#call fsys('pool_charset_test');
#call change_primary_zone('tenant_charset_test');

--enable_query_log

