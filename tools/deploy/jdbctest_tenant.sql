create resource unit test_box max_cpu 1, memory_size '10G', min_cpu 1;
create resource pool test_pool unit = 'test_box', unit_num = 1, zone_list = ('zone1');
create tenant mysql_tenant resource_pool_list=('test_pool'), primary_zone='RANDOM',locality = 'F@zone1' SET VARIABLES ob_compatibility_mode='mysql', ob_tcp_invited_nodes='%',secure_file_priv = "/", ob_query_timeout= 100000000000, ob_trx_timeout= 100000000000;

create resource unit test_oracle_box max_cpu 1, memory_size '10G', min_cpu 1;
create resource pool test_oracle_pool unit = 'test_oracle_box', unit_num = 1, zone_list = ('zone1');
create tenant oracle_tenant resource_pool_list=('test_oracle_pool'), primary_zone='RANDOM',locality = 'F@zone1' SET VARIABLES ob_compatibility_mode='oracle', ob_tcp_invited_nodes='%',secure_file_priv = "/", ob_query_timeout= 100000000000, ob_trx_timeout= 100000000000;
