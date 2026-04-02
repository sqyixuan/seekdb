use test;

set global binlog_row_image='MINIMAL';
alter system set writing_throttling_trigger_percentage=100;
alter system set writing_throttling_maximum_duration='2h';
set global ob_trx_idle_timeout=1200000000;
set global ob_query_timeout= 1000000000000;
set global ob_trx_timeout= 1000000000000;
set global recyclebin='OFF';
set global parallel_servers_target=100;
alter system set freeze_trigger_percentage=60 ;
set global binlog_row_image='MINIMAL';
set global auto_increment_cache_size=10000000;
alter system set default_auto_increment_mode = 'NOORDER';
