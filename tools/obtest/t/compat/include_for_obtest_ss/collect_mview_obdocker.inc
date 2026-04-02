connection tentsql_conn;
create database collect_mview;
use collect_mview;
create table collect_info(user_id int, item_id int, primary key(user_id, item_id)) partition by hash(user_id) partitions 4;
create table collect_item(item_id int, price int, primary key(item_id)) duplicate_scope = 'cluster' duplicate_read_consistency = 'weak';
insert into collect_item values(1, 100);
insert into collect_item values(2, 200);
insert into collect_item values(3, 300);
insert into collect_info values(1, 1);
insert into collect_info values(1, 2);
insert into collect_info values(2, 1);
insert into collect_info values(2, 3);
insert into collect_info values(3, 3);

create materialized view collect_info_mv (primary key(user_id, item_id)) partition by hash(user_id) partitions 4 refresh fast enable query rewrite enable on query computation as select collect_info.user_id, collect_info.item_id, collect_item.item_id dup_item_id, collect_item.price from collect_info inner join collect_item on collect_info.item_id = collect_item.item_id;
select * from collect_info_mv;
