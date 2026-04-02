set global parallel_servers_target=200;
alter system set major_compact_trigger=2;
alter system set freeze_trigger_percentage=30;
alter system set enable_early_lock_release=true;
alter system set undo_retention=600;
alter system set minor_compact_trigger=1;
alter system set ob_enable_batched_multi_statement=true;
alter system set open_cursors=65535;
set global max_allowed_packet = 219430400;
set global ob_query_timeout=10000000000;

