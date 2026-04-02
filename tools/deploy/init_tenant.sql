set @@session.ob_query_timeout = 200000000;
create resource unit box1 max_cpu=2, MEMORY_SIZE='1G', MIN_CPU=1;
create resource pool pool1 unit = 'box1', unit_num = 1;
create tenant ora_tt replica_num = 1, resource_pool_list=('pool1') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
set @@session.ob_query_timeout = 10000000;
alter tenant ora_tt set variables autocommit='on';
alter tenant ora_tt set variables nls_date_format='YYYY-MM-DD HH24:MI:SS';
alter tenant ora_tt set variables nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF';
alter tenant ora_tt set variables nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF TZR TZD';
