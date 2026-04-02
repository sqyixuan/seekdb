connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;

let $zone_count=query_get_value(select count(distinct zone) as cnt from oceanbase.__all_server, cnt, 1);

let $zone_index = 0;
let $port_1 = query_get_value(select inner_port from oceanbase.__all_server order by zone limit 1 offset $zone_index, inner_port, 1);
let $server_1 = query_get_value(select svr_ip from oceanbase.__all_server order by zone limit 1 offset $zone_index, svr_ip, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $port_2 = query_get_value(select inner_port from oceanbase.__all_server order by zone limit 1 offset $zone_index, inner_port, 1);
let $server_2 = query_get_value(select svr_ip from oceanbase.__all_server order by zone limit 1 offset $zone_index, svr_ip, 1);


let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $port_3 = query_get_value(select inner_port from oceanbase.__all_server order by zone limit 1 offset $zone_index, inner_port, 1);
let $server_3 = query_get_value(select svr_ip from oceanbase.__all_server order by zone limit 1 offset $zone_index, svr_ip, 1);


let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $port_4 = query_get_value(select inner_port from oceanbase.__all_server order by zone limit 1 offset $zone_index, inner_port, 1);
let $server_3 = query_get_value(select svr_ip from oceanbase.__all_server order by zone limit 1 offset $zone_index, svr_ip, 1);


disconnect obsys_ydm;
connection default;