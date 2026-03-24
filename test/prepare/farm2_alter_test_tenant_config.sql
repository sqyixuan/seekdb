use test;

set global ob_trx_idle_timeout=1200000000;
set global ob_query_timeout= 1000000000000;
set global ob_trx_timeout= 1000000000000;
set global recyclebin='OFF';
set global parallel_servers_target=100;
alter system set freeze_trigger_percentage=20 ;
set global binlog_row_image='MINIMAL';
