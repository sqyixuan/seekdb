create resource unit oracle_box max_cpu 2, memory_size '20G', min_cpu 2;
create resource pool oracle_pool unit = 'oracle_box', unit_num = 1, zone_list = ('zone1');
create tenant oracle_tenant resource_pool_list=('oracle_pool'), primary_zone='RANDOM',locality = 'F@zone1' SET VARIABLES ob_compatibility_mode='oracle', ob_tcp_invited_nodes='%',secure_file_priv = "/", ob_query_timeout= 100000000000, ob_trx_timeout= 100000000000;
