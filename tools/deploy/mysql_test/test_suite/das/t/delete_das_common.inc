--disable_warnings
drop table if exists t1, t_part, t_single;
--enable_warnings
--disable_warnings
drop table if exists t2;
--enable_warnings
--disable_warnings
drop table if exists t3;
--enable_warnings
--disable_warnings
drop table if exists t4;
--enable_warnings
#single column rowkey
create table t1(a int primary key, b int) partition by hash(a) partitions 2;
create index gkey on t1(b);

insert into t1 values(1,0),(2,0),(3,0);
delete from t1;
select * from t1;

insert into t1 values(1,0),(2,0),(3,0);
delete from t1 where a = 1;
delete from t1 where a = 2;
delete from t1 where a = 3;
select * from t1;

insert into t1 values(1,0),(2,0),(3,0);
delete from t1 where a=1;
select * from t1;

insert into t1 values(1,0);
replace into t1 values(2,0),(3,0);
delete from t1 where a=1 or a=2 or a=3;
select * from t1;

insert into t1 values(1,0),(2,0);
delete from t1 where a=1;
delete from t1 where a=2;
delete from t1 where a=3;
select * from t1;

drop table t1;

create table t_part (c1 bigint primary key, c2 bigint) partition by hash(c1) partitions 2;
create index gkey on t_part(c2);
create table t_single(c1 int);
insert into t_part(c1) values(1231346464513131);
insert into t_part values(1, 2), (3, 1);
insert into t_part values(2, 3), (4, 3);
insert into t_part partition(p0, p1) values(5, 6);
insert into t_part values('6', '6'), ('8', '8');
delete from t_part where c1 in (select c1 from t_single);
select * from t_part;
drop table t_part, t_single;

# multi-column rowkey
create table t2 (p1 int, p2 int, p3 int, p4 int, primary key(p1,p2,p3)) partition by hash(p1) partitions 2;
create index gkey on t2(p3); 

insert into t2 values(1,1,1,0),(2,2,2,0),(3,3,3,0),(4,4,4,0),(5,4,4,0),(6,4,4,0),(7,4,6,0),(8,4,6,0),(9,4,6,0),(10,4,6,0),(11,4,6,0),(12,4,6,0), (13,4,6,0);

delete from t2 where p1 = 1;

--disable_query_log
--let $p=13
while($p>1)
{
   eval delete from t2 where p1=$p and p2 = $p and p3=$p;
   dec $p;
}
--enable_query_log

select * from t2;
select p1,p3 from t2;

replace into t2 values(1,1,1,0),(2,2,2,0),(3,3,3,0),(4,4,4,0),(5,4,4,0),(6,4,4,0),(7,4,6,0),(8,4,6,0),(9,4,6,0),(10,4,6,0),(11,4,6,0),(12,4,6,0), (13,4,6,0);

delete from t2 where p1>3 or p3 >=6;
select * from t2;
select p1,p3 from t2;
delete from t2 where p1=1 and p3 =2;
select * from t2;
select p1,p3 from t2;
delete from t2 where p1=1 and p2 =1 and p3=1;
select * from t2;
select p1,p3 from t2;

replace into t2 values(1,1,1,0),(2,2,2,0),(3,3,3,0),(4,4,4,0),(5,4,4,0),(6,4,4,0),(7,4,6,0),(8,4,6,0),(9,4,6,0),(10,4,6,0),(11,4,6,0),(12,4,6,0), (13,4,6,0);

delete from t2 where p1 in (1,2,3,6,7,8,12,13,0);
select * from t2;

drop table t2;


# mix common column and rowkey column
create table t3 (p1 int, p2 int, p3 int, primary key(p1,p2)) partition by hash(p2) partitions 2;
create index gkey on t3(p3);

insert into t3 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,4,4),(6,4,4),(7,4,6),(8,4,6),(9,4,6),(10,4,6),(11,4,6),(12,4,6), (13,4,6);
delete from t3 where p1=1 and p2=1 and p3=1;
select * from t3;
select p1,p3 from t3;

replace into t3 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,4,4),(6,4,4),(7,4,6),(8,4,6),(9,4,6),(10,4,6),(11,4,6),(12,4,6), (13,4,6);
delete from t3 where p1>3 or p3 >=6;
select * from t3;
select p1,p3 from t3;

delete from t3 where p1=1 and p3 =2;
select * from t3;
select p1,p3 from t3;

replace into t3 values(1,1,1),(2,2,2);
delete from t3 where p1=1 and p2 =1 and p3=1;
select * from t3;
select p1,p3 from t3;

replace into t3 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,4,4),(6,4,4),(7,4,6),(8,4,6),(9,4,6),(10,4,6),(11,4,6),(12,4,6),(13,4,6);
delete from t3 where p1 in (1,2,3,6,7,8,12,13,0);
select * from t3;

drop table t3;

# where clause
create table t4 (p1 varchar(100), p2 int, p3 datetime(6), p4 int, primary key(p1,p2,p3)) partition by key(p1) partitions 2;
create index gkey on t4(p2);

insert into t4 values('a', 41, '2012-10-23 17:14:00',0), ('b', 42, '2012-10-23 17:15:00',0), ('c', 43, '2012-10-23 17:16:00',0);
delete from t4 where p3='2012-10-23 17:14:00';
select * from t4;

replace into t4 values('a', 41, '2012-10-23 17:14:00',0), ('b', 42, '2012-10-23 17:15:00',0), ('c', 43, '2012-10-23 17:16:00',0);
delete from t4 where p2='42';
select * from t4;

replace into t4 values('a', 41, '2012-10-23 17:14:00',0), ('b', 42, '2012-10-23 17:15:00',0), ('c', 43, '2012-10-23 17:16:00',0);
delete from t4 where p2<'401' and p1='c';
select * from t4;

delete from t4 where p1='a' and p2=41 and p3='2012-10-23 17:14:00';
select * from t4;

drop table t4;

create table t2(c1 int primary key, c2 int, c3 int) partition by hash(c1) partitions 2;
create index gkey on t2(c2);
create table t3 (c1 int, c2 int, c3 int);
create unique index guk on t3(c1) partition by hash(c1) partitions 2; 

insert into t2 values (1, 1, 1);
insert into t2 values (2, 2, 2);
insert into t2 values (3, 2, 3);
insert into t2 values (4, 6, 4);
insert into t2 values (5, 4, 3);
insert into t2 values (6, 4, 2);
insert into t3 values(2, 2, 2);
insert into t3 values(3, 3, 3);
insert into t3 values(4, 4, 4);
insert into t3 values(5, 5, 5);

delete t3 from t3, t2 where t3.c1 = t2.c2;
delete t3 from t3, t2 where t3.c1 = t2.c1;
select * from t3;
select * from t2;

drop table t2;
drop table t3;

--disable_warnings
drop table if exists dns_inner_resource_record_info;
--enable_warnings

CREATE TABLE `dns_inner_resource_record_info`( 
         `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键', 
         `owner` varchar(256) NOT NULL COMMENT '资源',
         `cluster_name` varchar(20) NOT NULL COMMENT '所属集群 冗余字段', 
         `zone_name` varchar(255) NOT NULL DEFAULT '' COMMENT 'ZONE NAME', 
         `view_group_name` varchar(60) DEFAULT '' COMMENT 'VIEW GRP name', 
         `domain_group_name` varchar(60) DEFAULT NULL COMMENT '域名组', 
         `idc_id` varchar(20) DEFAULT NULL COMMENT 'IDC ID',
         `rr_type` varchar(10) NOT NULL COMMENT '资源类型', 
         `ttl` varchar(20) NOT NULL DEFAULT '' COMMENT 'TTL', 
         `rr_class` varchar(12) NOT NULL DEFAULT '' COMMENT 'RECODR 类型', 
         `data` varchar(500) NOT NULL COMMENT '数据', 
         `status` varchar(10) DEFAULT NULL COMMENT '状态', 
         `gmt_create` datetime NOT NULL COMMENT '创建时间', 
         `gmt_modified` datetime DEFAULT NULL COMMENT '修改时间',
         PRIMARY KEY (`id`),
         KEY `rrdata` (`data`(383)) BLOCK_SIZE 16384 GLOBAL,
         KEY `rrtype` (`rr_type`) BLOCK_SIZE 16384 GLOBAL,
         KEY `owner` (`owner`) BLOCK_SIZE 16384 GLOBAL,
         KEY `zonename` (`zone_name`) BLOCK_SIZE 16384 GLOBAL,
         KEY `viewgroupname` (`view_group_name`) BLOCK_SIZE 16384 GLOBAL,
         KEY `domain_group_name` (`domain_group_name`) BLOCK_SIZE 16384 GLOBAL,
         KEY `idc_id` (`idc_id`) BLOCK_SIZE 16384 GLOBAL
         ) COMMENT = 'dns 内网集群资源记录表';
create index cluster on dns_inner_resource_record_info(cluster_name) partition by key(cluster_name) partitions 2;
insert into dns_inner_resource_record_info values(null, 'a', '1', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', now(), now());
delete from dns_inner_resource_record_info WHERE cluster_name = "1";
set binlog_row_image='MINIMAL';
delete from dns_inner_resource_record_info WHERE cluster_name = "1";
drop table dns_inner_resource_record_info;

create table t1(a int primary key, b int) partition by hash(a) partitions 2;
create table t2(a int primary key, b int) partition by hash(a) partitions 2;
insert into t2 values(0, 0), (2, 2), (4, 4);
insert into t1 select * from t2;
select * from t1;
drop table t1;
drop table t2;

--disable_warnings
drop table if exists t14, t15;
--enable_warnings
create table t14(a int, b int, primary key(a)) partition by hash(a) partitions 2;
create table t15(a int, b int, primary key(a)) partition by hash(a) partitions 2;
insert into t14 values(1, 1), (2, 2), (3, 3), (4, 4);
insert into t15 values(1, 1), (2, 2), (3, 3);
select * from t14;
select * from t15;
update t14 set b=100 where a in (select a from t15);
select * from t14;
update t14 set b=b-10 where a in (select a from t15 where t14.a=1);
select * from t14;
update t14 set b=(select b from t15 where t14.b=t15.b+86) where a in (select a from t15);
update t14 set b=(select b from t15 where t15.b=4) where a in (select a from t15);
select * from t14;
update t14 set a=100 where a in (select b from t14);

# @todo laibingzheng.lbz
# this maybe fail,if wesubmit das task each one row
#update t14 set a=a+1 where a in (select a from t15);
update t14 set a=a+1 where a = 3;
update t14 set a=a+1 where a = 2;
update t14 set a=a+1 where a = 1;
select * from t14;
--error 1062
update t14 set a=a+1, b=20 where a in (select a from t15);
select * from t14;

create index gkey on t14(b);
create index gkey on t15(b);
delete from t14 where a in (select b from t14);
select * from t14;
delete from t14 where a in (select a+2 from t15);
select * from t14;
delete from t14 where b in (select 1 from t15);
delete from t14 where b in (select a from t15);
delete from t14 where a in (select a from t15);
select * from t14;
select * from t15;
drop table t14;
drop table t15;
