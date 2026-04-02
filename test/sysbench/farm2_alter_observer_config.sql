use oceanbase;

## max_cpu = min_cpu
alter resource unit sys_unit_config min_cpu=1,max_cpu=1;


alter system set system_memory='6g';
alter proxyconfig set enable_compression_protocol = false ;
alter proxyconfig set proxy_mem_limited='4G';
alter proxyconfig set enable_qos=false;
alter system set _ob_enable_prepared_statement=true;
