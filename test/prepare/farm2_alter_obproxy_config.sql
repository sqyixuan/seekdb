use oceanbase;

alter proxyconfig set server_detect_fail_threshold=300;

alter proxyconfig set client_max_connections=65535;
alter proxyconfig set enable_reroute=True;
alter proxyconfig set enable_index_route=True;
alter proxyconfig set proxy_mem_limited='4G';

