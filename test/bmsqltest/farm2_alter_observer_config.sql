use oceanbase;
alter resource unit sys_unit_config min_cpu=1,max_cpu=1;
alter system set system_memory='6g';
alter system set enable_record_trace_log = false;
alter system ignore_replay_checksum_error = true;
alter system set enable_monotonic_weak_read = false;
alter system set weak_read_version_refresh_interval='2s'; 
