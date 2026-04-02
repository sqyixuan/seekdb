
set ob_query_timeout= 100000000000;
select sleep(5);
use oceanbase;

set global ob_enable_transmission_checksum=OFF;
set global ob_trx_timeout= 100000000000;
set global ob_query_timeout= 100000000000;

set global recyclebin=off;
set global max_allowed_packet=67108864;
set global ob_plan_cache_percentage = 1;
set global autocommit=ON;
alter system set major_compact_trigger=1 tenant=sys;
alter system set major_compact_trigger=1 tenant=all_user;
alter system set major_compact_trigger=1 tenant=all_meta;
alter system set freeze_trigger_percentage=5 tenant=sys;
alter system set freeze_trigger_percentage=5 tenant=all_user;
alter system set freeze_trigger_percentage=5 tenant=all_meta;
alter system set large_query_threshold='100s';
