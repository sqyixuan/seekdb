--disable_query_log
--disable_result_log
drop table if exists kv;
create table kv (k varchar(100), v varchar(200));
# agg func
insert into kv values('agg_func', 'count');
insert into kv values('agg_func', 'sum');
insert into kv values('agg_func', 'avg');
insert into kv values('agg_func', 'min');
insert into kv values('agg_func', 'max');
insert into kv values('agg_func', 'group_concat');
--enable_query_log
--enable_result_log
