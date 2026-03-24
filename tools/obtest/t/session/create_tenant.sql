##################################### create tenant #####################################
--error 0,5157
drop tenant yts_tt force;
let $cpu = 1;
let $memory = 1073741824;
let $min_memory = 1073741824;
let $disk = 536870912;
let $iops = 100000;
let $session = 64;
--disable_warnings
--disable_query_log
--disable_result_log
eval create resource unit if not exists yts_box max_cpu 3, max_memory $memory, max_iops $iops, max_disk_size $disk, max_session_num $session, MIN_CPU= 2, MIN_MEMORY=$min_memory, MIN_IOPS=128;
let $zone_name=query_get_value(select zone from oceanbase.__all_zone where zone != '' limit 1, zone, 1);

let $obs_num = query_get_value(select count(1) as cnt from oceanbase.__all_server group by zone asc limit 1,cnt, 1);
sleep 10;
eval create resource pool if not exists yts_pool unit = 'yts_box', unit_num = $obs_num;
eval create tenant if not exists yts_tt primary_zone='$zone_name', resource_pool_list('yts_pool');

sleep 5;
--error 0,1064
alter tenant yts_tt set variables ob_tcp_invited_nodes='%';

--enable_warnings
--enable_query_log
--enable_result_log
sleep 5;
