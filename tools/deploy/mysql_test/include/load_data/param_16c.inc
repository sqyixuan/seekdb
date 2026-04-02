alter system set memory_limit = '100G';
alter system set cpu_count = 16; 

let $char_charset = 'utf8';
let $cpu = 8;
let $memory = '50G';
let $disk = '100G';
let $iops = 100000;
let $session = 64;
let $parallel_servers_target = 10;
let $ob_sql_work_area_percentage = 20;
