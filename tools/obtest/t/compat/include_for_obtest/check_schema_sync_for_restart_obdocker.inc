--disable_query_log
obdocker.check_until_timeout select count(*) from oceanbase.__all_server a left join oceanbase.__all_virtual_server_schema_info b on a.svr_ip = b.svr_ip and a.svr_port = b.svr_port where b.svr_ip is null, 0, 180;
obdocker.check_until_timeout select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where a.refreshed_schema_version != b.refreshed_schema_version or a.refreshed_schema_version % 8 != 0, 0, 180;
--enable_query_log
