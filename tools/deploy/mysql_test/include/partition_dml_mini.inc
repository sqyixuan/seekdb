# Mini : Test dml for partitioned tables with different partition types
--disable_warnings
drop table if exists t1,t2;
--enable_warnings
eval CREATE TABLE t1 (
	c1 INT,
	c2 VARCHAR(100),
	c3 BIGINT ,
	PRIMARY KEY(c1,c2),
	KEY(c1) LOCAL)
	PARTITION BY $part_type (c1) PARTITIONS 4;

eval CREATE TABLE t2 (
	c1 INT,
	c2 VARCHAR(100),
	c3 BIGINT ,
	PRIMARY KEY(c1,c2),
	KEY(c1) LOCAL)
	PARTITION BY $part_type (c1) PARTITIONS 4;


#Insert data
let $cnt = 8;
--disable_query_log
while ($cnt)
{
	eval REPLACE INTO t1 VALUES($cnt, 'C$cnt', $cnt * $cnt);
	eval INSERT  INTO t1 VALUES($cnt + 10000, 'C$cnt', $cnt * $cnt + 10000);
	dec $cnt;
}
let $cnt = 16;
--disable_query_log
while ($cnt > 8)
{
	eval REPLACE INTO t1 VALUES($cnt, 'C$cnt', $cnt * $cnt);
	eval INSERT  INTO t1 VALUES($cnt + 10000, 'C$cnt', $cnt * $cnt + 10000);
	dec $cnt;
}
--enable_query_log

# In the following sections, needs to cover test of select, update, delete, join, union

# - select
#explain partitions select * from t1 where c1 = 3;
select * from t1 where c1 = 3;

#explain partitions select * from t1 where c1 <=> 3;
select * from t1 where c1 <=> 3;

#explain partitions select * from t1 where c1 = 3 or c1 = 4;
select * from t1 where c1 = 3 or c1 = 4;

#explain partitions select * from t1 where c1 > 98;
select * from t1 where c1 > 98;

#explain partitions select * from t1 where c1 >=98;
select * from t1 where c1 >=98;

#explain partitions select * from t1 where c1 < 3;
select * from t1 where c1 < 3;

#explain partitions select * from t1 where c1 <= 3;
select * from t1 where c1 <= 3;

#explain partitions select * from t1 where c1 <=3 and c1>=1;
select * from t1 where c1 <=3 and c1>=1;

#explain partitions select * from t1 partition(p0);
select * from t1 partition(p0);
select * from t1 partition(p0,p1);
select * from t1;

# - update
#explain partitions update t1 set c2=concat(c2,'_up') where c1=1;
update t1 set c2=concat(c2,'_up') where c1=1;

#explain partitions update t1 set c2=concat(c2,'_up') where c1<=>1;
update t1 set c2=concat(c2,'_up') where c1<=>1;

explain partitions update t1 set c2=concat(c2,'_up') where c1=2 or c3=3;
update t1 set c2=concat(c2,'_up') where c1=2 or c3=3;

explain partitions update t1 set c2=concat(c2,'_up') where c1>98;
update t1 set c2=concat(c2,'_up') where c1>98;

explain partitions update t1 set c2=concat(c2,'_up') where c1>=98;
update t1 set c2=concat(c2,'_up') where c1>=98;

explain partitions update t1 set c2=concat(c2,'_up') where c1<3;
update t1 set c2=concat(c2,'_up') where c1<3;

explain partitions update t1 set c2=concat(c2,'_up') where c1<=3;
update t1 set c2=concat(c2,'_up') where c1<=3;

explain partitions update t1 set c2=concat(c2,'_up') where c1<=3 and c1>=1;
update t1 set c2=concat(c2,'_up') where c1<=3 and c1>=1;

explain partitions update t1 set c2=concat(c2,'_up') where c2>'C90';
update t1 set c2=concat(c2,'_up') where c2>'C90';

# - delete
#explain partitions delete from t1 where c1 = 3;
delete from t1 where c1 = 3;

delete from t1 where c1 <=> 4;

explain partitions delete from t1 where c1 = 5 or c1 = 4;
delete from t1 where c1 = 5 or c1 = 4;

explain partitions delete from t1 where c1 > 98;
delete from t1 where c1 > 98;

explain partitions delete from t1 where c1 >= 98;
delete from t1 where c1 >= 98;

explain partitions delete from t1 where c1<3;
delete from t1 where c1<3;

explain partitions delete from t1 where c1<=3;
delete from t1 where c1<=3;

explain partitions delete from t1 where c1>=6 and c1<=8;
delete from t1 where c1>=6 and c1<=8;

#explain partitions delete from t1 partition(p0);
delete from t1 partition(p0);
delete from t1 partition(p0,p1);
delete from t1;
