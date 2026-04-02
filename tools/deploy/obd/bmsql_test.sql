create resource unit oracle_box max_cpu 2, memory_size 32212254720;
create resource pool oracle_pool unit = 'oracle_box', unit_num = 1, zone_list = ('z1');
create tenant oracle_tenant resource_pool_list=('oracle_pool'), primary_zone='RANDOM',locality = 'F@z1' SET VARIABLES ob_compatibility_mode='oracle', ob_tcp_invited_nodes='%',secure_file_priv = "/";

set global ob_query_timeout= 100000000000;
set global ob_trx_timeout= 100000000000;
set global ob_enable_transmission_checksum=OFF;

set global recyclebin=off;
set global parallel_servers_target=600;
set global max_allowed_packet=67108864;
set global ob_plan_cache_percentage = 1;
set global autocommit=ON;
