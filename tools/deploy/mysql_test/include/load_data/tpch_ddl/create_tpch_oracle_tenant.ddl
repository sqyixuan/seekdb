CREATE RESOURCE UNIT tpch_oracle10 MAX_CPU=16, min_cpu=16, MEMORY_SIZE='32G';
CREATE RESOURCE POOL tpch_oracle10 unit='tpch_oracle10', unit_num=1;
CREATE TENANT tpch_oracle10 RESOURCE_POOL_LIST=('tpch_oracle10'),primary_zone='z1'set ob_compatibility_mode='oracle';
alter tenant tpch_oracle10 set variables ob_tcp_invited_nodes='%';
alter system set trace_log_slow_query_watermark='100s';
