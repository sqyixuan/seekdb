use oceanbase;

## max_cpu = min_cpu
#alter resource unit sys_unit_config min_cpu=1,max_cpu=1;
#alter resource unit sys_unit_config max_cpu=2.5;
#alter system set minor_freeze_times=10;

## bmsql
#alter system set _max_trx_size='100G';
#alter system set enable_merge_by_turn=false;

## quick recover 
#alter system set election_blacklist_interval='30s';

## 500 tenant
alter system set system_memory='6g';

## frequently dump
#alter system set freeze_trigger_percentage=20;

## easy debug
alter system set syslog_io_bandwidth_limit='3000m';

## reduce cpu%
alter system set weak_read_version_refresh_interval='1s';


alter system set memory_chunk_cache_size ='1G';
