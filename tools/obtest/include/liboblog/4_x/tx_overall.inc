##############
#owner: bohou.ws
#test points: all kinds of tx behave
##############

connection conn_obcdc_mysql;

set ob_query_timeout=90000000;
set ob_trx_timeout=1200000000;
set autocommit=0;

create database if not exists obcdc_db;

use obcdc_db;
# t_normal: 1KB for each row
let $size=1024;
eval create table t_normal(c1 int primary key, c2 varchar($size));

############# case 1 #############
## single ls trans 1MB, all redo in one LogEntry
let $cnt = 1024;
let $t_idx=0;

--disable_query_log
#begin;
#while ($cnt > 0)
  #{
  #  eval insert into t_normal values ($t_idx, repeat('a', $size));
  #  inc $t_idx;
  #  dec $cnt;
  #}
  #commit;
--enable_query_log
truncate t_normal;

############# case 2 #############

## single ls trans 10MB, redo in one LogEntry
let $cnt = 10240;
let $t_idx=0;
--disable_query_log
#begin;
#while ($cnt > 0)
  #{
  #  eval insert into t_normal values ($t_idx, repeat('a', $size));
  #  dec $cnt;
  #}
  #commit;
--enable_query_log
#drop table t_normal;


# t_big: 100KB for each row
## 100k for each varchar column, 2 varchar column in each row
let $size = 51200;
eval create table t_big(c1 int primary key, c2 varchar($size), c3 varchar($size)) partition by hash (c1) partitions 10;

############# case 3 #############
## trans 1GB: multi redo_log_entry and may be dist trans if tenant has multi ls
let $cnt = 1024;
let $t_idx=0;
--disable_query_log
begin;
while ($cnt > 0)
{
  if ($t_idx == 1)
  {
    let $restart_ts1_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
  }
  eval insert into t_big values ($t_idx, repeat('a', $size), repeat('b', $size));
  if ($cnt == 1)
  {
    let $restart_ts2_usec = query_get_value(select time_to_usec(now(6)) as now, now, 1);
  }
  inc $t_idx;
  dec $cnt;
}
commit;
--enable_query_log
drop database obcdc_db;

set autocommit=1;
