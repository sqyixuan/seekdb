
connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;

let $zone_count=query_get_value(select count(distinct zone) as cnt from oceanbase.__all_server, cnt, 1);

let $zone_index = 0;
let $zone_1 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_2 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_3 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_4 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_5 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_6 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_7 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_8 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

let $zone_index = query_get_value(select ($zone_index + 1) % $zone_count as zone_index, zone_index, 1);
let $zone_9 = query_get_value(select distinct zone from oceanbase.__all_server order by zone limit 1 offset $zone_index, zone, 1);

disconnect obsys_ydm;
connection default;

