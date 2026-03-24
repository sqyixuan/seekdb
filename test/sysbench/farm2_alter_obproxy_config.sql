use oceanbase;

alter proxyconfig set server_detect_fail_threshold=300;

alter proxyconfig set client_max_connections=65535;
alter proxyconfig set proxy_mem_limited='100G';
alter proxyconfig set enable_ob_protocol_v2=false;
alter proxyconfig set enable_reroute=True;
alter proxyconfig set enable_index_route=True;
alter proxyconfig set enable_compression_protocol = false;
alter proxyconfig set proxy_mem_limited='4G';
alter proxyconfig set enable_qos=false;

