##################################### create tenant #####################################
--error 0,5157
drop tenant yts_tt force;
--disable_warnings
--disable_query_log
--disable_result_log
eval create resource unit if not exists yts_box max_cpu 3, memory_size '1G';
let $zone_name=query_get_value(select zone from oceanbase.DBA_OB_ZONES where zone != '' limit 1, zone, 1);

let $obs_num = query_get_value(select count(1) as cnt from oceanbase.DBA_OB_SERVERS group by zone asc limit 1,cnt, 1);
sleep 10;
eval create resource pool if not exists yts_pool unit = 'yts_box', unit_num = $obs_num;
eval create tenant if not exists yts_tt primary_zone='$zone_name', resource_pool_list('yts_pool');
    sleep 5;
    --error 0, 1064
    alter tenant yts_tt set variables ob_tcp_invited_nodes='%';

    sleep 5;
    --enable_warnings
    connect (conn1,$OBMYSQL_MS0,root@yts_tt,,*NO-ONE*,$OBMYSQL_PORT);
    connect (conn_admin,$OBMYSQL_MS0,admin@sys,admin,*NO-ONE*,$OBMYSQL_PORT);
    --enable_query_log
    --enable_result_log
    connection conn1;
    select effective_tenant();
