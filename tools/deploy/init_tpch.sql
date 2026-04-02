system sleep 5;
alter system set  balancer_idle_time = '60s';
create user if not exists 'admin' IDENTIFIED BY 'admin';
use oceanbase;
create database if not exists test;

use test;
grant all on *.* to 'admin' WITH GRANT OPTION;


create user if not exists 'proxyro' IDENTIFIED BY '3u^0kCdpE';
grant select on oceanbase.* to proxyro IDENTIFIED BY '3u^0kCdpE';


set global ob_enable_jit='OFF';
alter system set large_query_threshold='1s';
alter system set syslog_level='info';
alter system set syslog_io_bandwidth_limit='30M';
alter system  set trace_log_slow_query_watermark='500ms';
alter system set clog_sync_time_warn_threshold = '1000ms';
alter system set trace_log_slow_query_watermark = '10s';
alter system set enable_sql_operator_dump = 'false';
alter system set rpc_timeout=1000000000;


set @@session.ob_query_timeout = 200000000;
create resource unit tpch_box1 memory_size '100g', log_disk_size '100g', min_cpu=9, max_cpu=9, max_iops 100000, min_iops=10000;
create resource pool tpch_pool1 unit = 'tpch_box1', unit_num = 1, zone_list = ('z1', 'z2', 'z3');
create tenant oracle replica_num = 3, resource_pool_list=('tpch_pool1') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
set @@session.ob_query_timeout = 10000000;

alter tenant oracle set variables autocommit='on';
alter tenant oracle set variables nls_date_format='yyyy-mm-dd hh24:mi:ss';
alter tenant oracle set variables nls_timestamp_format='yyyy-mm-dd hh24:mi:ss.ff';
alter tenant oracle set variables nls_timestamp_tz_format='yyyy-mm-dd hh24:mi:ss.ff tzr tzd';
alter tenant oracle set variables ob_query_timeout=7200000000;
alter tenant oracle set variables ob_trx_timeout=7200000000;
alter tenant oracle set variables max_allowed_packet=67108864;
alter tenant oracle set variables ob_enable_jit='OFF';
alter tenant oracle set variables ob_sql_work_area_percentage=80;
alter tenant oracle set variables parallel_servers_target=512;

delimiter //
drop procedure if exists fsys;//
create procedure fsys(tenant_name varchar(64))
begin
 declare num int;
  select count(*) from oceanbase.__all_server group by zone limit 1 into num;
  set @sql_text = concat('alter resource tenant ',  tenant_name, ' unit_num = ', num);
  prepare stmt from @sql_text;
  execute stmt;
  deallocate prepare stmt;
  -- select num;
end //

delimiter ;
call fsys('oracle');
