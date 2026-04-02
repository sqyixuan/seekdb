# Test dml for partitioned tables with different partition types

--disable_warnings
drop table if exists t1,t2;
--enable_warnings
eval CREATE TABLE t1 (c1 INT,	c2 VARCHAR(100), c3 BIGINT,	PRIMARY KEY(c1,c2),	KEY(c1) LOCAL) PARTITION BY $part_type (c1) PARTITIONS 4;
eval CREATE TABLE t2 (c1 INT,	c2 VARCHAR(100), c3 BIGINT,	PRIMARY KEY(c1,c2),	KEY(c1) LOCAL) PARTITION BY $part_type (c1) PARTITIONS 4;

if ($part_type == 'RANGE')
{
--disable_warnings
drop table if exists t1,t2;
--enable_warnings
eval CREATE TABLE t1 (c1 INT,	c2 VARCHAR(100), c3 BIGINT,	PRIMARY KEY(c1,c2),	KEY(c1) LOCAL) PARTITION BY $part_type (c1) PARTITIONS 4
( partition p0 values less than 3,
  partition p0 values less than 5,
  partition p0 values less than 7,
  partition p0 values less than maxvalue
);
eval CREATE TABLE t2 (c1 INT,	c2 VARCHAR(100), c3 BIGINT,	PRIMARY KEY(c1,c2),	KEY(c1) LOCAL) PARTITION BY $part_type (c1) PARTITIONS 4
( partition p0 values less than 3,
  partition p0 values less than 5,
  partition p0 values less than 7,
  partition p0 values less than maxvalue
);
}


#Insert data
let $cnt = 8;
#--disable_query_log
while ($cnt)
{
	eval REPLACE INTO t1 VALUES($cnt, 'C$cnt', $cnt * $cnt);
	eval INSERT  INTO t1 VALUES($cnt + 10000, 'C$cnt', $cnt * $cnt + 10000);
	dec $cnt;
}
let $cnt = 16;
#--disable_query_log
while ($cnt > 8)
{
	eval REPLACE INTO t2 VALUES($cnt, 'C$cnt', $cnt * $cnt);
	eval INSERT  INTO t2 VALUES($cnt + 10000, 'C$cnt', $cnt * $cnt + 10000);
	dec $cnt;
}
#--enable_query_log

# In the following sections, needs to cover test of select, update, delete, join, union

# - select
# @TODO single partition. Plan is different when partition is local or remote.
#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  select * from t1 where c1 = 3;
select * from t1 where c1 = 3;

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  select * from t1 where c1 <=> 3;
select * from t1 where c1 <=> 3;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 where c1 = 3 or c1 = 0;
select * from t1 where c1 = 3 or c1 = 0;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 where c1 > 98;
select * from t1 where c1 > 98;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 where c1 >=98;
select * from t1 where c1 >=98;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 where c1 < 3;
select * from t1 where c1 < 3;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 where c1 <= 3;
select * from t1 where c1 <= 3;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 where c1 <=3 and c1>=1;
select * from t1 where c1 <=3 and c1>=1;

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  select * from t1 partition(p0);
select * from t1 partition(p0);
# bug: 
select * from t1 partition(p0,p1);
select * from t1;

# test select in desc order
select * from t1 order by c1 desc;
select * from t1 partition(p0,p1) order by c1 desc;

# Test order by elimination
select * from t1 where c1 = 1 or c1 = 2 order by c1;
select * from t1 where c1 = 1 or c1 = 2 order by c1 desc;

# Test sort elimination with group by
select * from t1 where c1 = 1 or c1 = 2 group by c1;

# - update
#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  update t1 set c2=concat(c2,'_up') where c1=1;
update t1 set c2=concat(c2,'_up') where c1=1;

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  update t1 set c2=concat(c2,'_up') where c1<=>1;
update t1 set c2=concat(c2,'_up') where c1<=>1;

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  update t1 set c2=concat(c2,'_up') where c1=2;
update t1 set c2=concat(c2,'_up') where c1=2;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  update t1 set c2=concat(c2,'_up') where c2='C90';
update t1 set c2=concat(c2,'_up') where c2='C90';

# - delete
#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  delete from t1 where c1 = 3;
delete from t1 where c1 = 3;

delete from t1 where c1 <=> 4;
select * from t1;

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  delete from t1 where c1 = 3 ;
delete from t1 where c1 = 3 ;
select * from t1;

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  delete from t1 partition(p0);
delete from t1 partition(p0);
delete from t1 partition(p0,p1);
delete from t1;
select * from t1;

# - join
--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t2.c1<15;
#--error 4016
select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t2.c1<15;

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t1.c1 >=8 and t2.c1<15;
select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t1.c1 >=8 and t2.c1<15;

select * from t1,t2 where t1.c1 + 8 = t2.c1;
select * from t1 inner join t2 on t1.c1 + 8 = t2.c1;
select * from t1 inner join t2 using(c1);
select * from t1 left join t2 on t1.c1 + 8 = t2.c1;
--sorted_result
select * from t1 right join t2 on t1.c1 + 8 = t2.c1;
select * from t1 left join t2 on t1.c1 + 8 = t2.c1 where c1 between 6 and 10;
selecct * from t1 inner join t1 as t3 on t1.c1 = t3.c2 left join t1 t4 on t1.c1 = t4.c1;
selecct * from t1 inner join t1 as t3 on t1.c1 = t3.c2 left join t1 t4 on t1.c1 = t4.c1 left join t2 on t1.c1 + 8 = t2.c1;

# - union
#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c2='C1' );
( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c2='C1' );

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  ( select * from t1 where c1>10 ) union ( select * from t2 where c1>1 and c2<'C5' );
( select * from t1 where c1>10 ) union ( select * from t2 where c1>1 and c2<'C5' );

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c2<=>'C1' );
( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c2<=>'C1' );

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c1<=3 );
( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c2<=3 );


#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1=1 ) union all ( select * from t2 where c1>1 and c2='C1' );
( select * from t1 where c1=1 ) union all ( select * from t2 where c1>1 and c2='C1' );

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  ( select * from t1 where c1>10 ) union all ( select * from t2 where c1>1 and c2<'C5' );
( select * from t1 where c1>10 ) union all ( select * from t2 where c1>1 and c2<'C5' );

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1<=>1 ) union all ( select * from t2 where c1>1 and c2<=>'C1' );
( select * from t1 where c1<=>1 ) union all ( select * from t2 where c1>1 and c2<=>'C1' );

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1<=>1 ) union all ( select * from t2 where c1>1 and c1<=3 );
( select * from t1 where c1<=>1 ) union all ( select * from t2 where c1>1 and c2<=3 );


#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1=1 ) union distinct ( select * from t2 where c1>1 and c2='C1' );
( select * from t1 where c1=1 ) union distinct ( select * from t2 where c1>1 and c2='C1' );

--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
explain  ( select * from t1 where c1>10 ) union distinct ( select * from t2 where c1>1 and c2<'C5' );
( select * from t1 where c1>10 ) union distinct ( select * from t2 where c1>1 and c2<'C5' );

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1<=>1 ) union distinct ( select * from t2 where c1>1 and c2<=>'C1' );
( select * from t1 where c1<=>1 ) union distinct ( select * from t2 where c1>1 and c2<=>'C1' );

#--replace_regex /Plan signature: [0-9]{1,}/Plan signature:/
#explain  ( select * from t1 where c1<=>1 ) union distinct ( select * from t2 where c1>1 and c1<=3 );
( select * from t1 where c1<=>1 ) union distinct ( select * from t2 where c1>1 and c2<=3 );


