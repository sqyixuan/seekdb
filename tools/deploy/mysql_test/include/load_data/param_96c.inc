alter system set memory_limit='710G';
alter system set memstore_limit_percentage = 80;

let $cpu = 40;
let $memory = '350G';
let $disk = '100G';
let $iops = 10240;
let $session = 1024;
let $char_charset = 'utf8';
let $parallel_servers_target = 10649;
let $ob_sql_work_area_percentage = 20;
