set @@recyclebin = off;
drop table if exists t1, t2, t3, ts1, ts2, tu1, tu2, td1, tdd1, td2, td3, td4;
drop tablegroup if exists group1;
create tablegroup group1;
create table t1(c1 int primary key, c2 int, c3 int, key(c2), key(c3,c2)) tablegroup='group1';
create table t2(c1 int primary key, c2 int, c3 int, key(c2), key(c3,c2)) tablegroup='group1';
create table t3(c1 int primary key, c2 int, c3 int, key(c2), key(c3,c2)) tablegroup='group1';
insert into t1 value(1, 2, 4), (2,2,3), (3,2,2), (4,2,1);
insert into t1 value(11, 3, 4), (12,3,3), (13,3,2), (14,3,1), (15,4,1);
insert into t2 value(21, 4, 4), (22,4,3), (23,4,2), (24,4,1);
insert into t3 value(21, 4, 4), (22,4,3), (23,4,2), (24,4,1);

##for insert,update,delete
#insert
create table ts1(c1 int primary key, c2 int) tablegroup='group1';
create table ts2(c1 int primary key, c2 int) tablegroup='group1';
#update
create table tu1(c1 int primary key, c2 int, c3 varchar(20)) tablegroup='group1';
create table tu2(c1 int primary key, c2 int, c3 varchar(20)) tablegroup='group1';
#delete
create table td1(c1 int primary key, c2 int) tablegroup='group1';
create table tdd1(c1 int primary key, c2 int) tablegroup='group1';
create table td2(c1 int primary key, c2 int) tablegroup='group1';
create table td3 (p1 int, p2 int, p3 int, p4 int, primary key(p1,p2,p3)) tablegroup='group1';
create table td4(a int, b varchar(128), c  timestamp(6) default "2012-01-01 12:00:00", d int, primary key(a,b,c)) tablegroup='group1';
