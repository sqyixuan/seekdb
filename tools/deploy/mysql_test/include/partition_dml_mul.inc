# Test dml for PARTITION BY KEY(), with multiple columns in key()
--disable_warnings
drop table if exists t1,t2;
--enable_warnings
eval CREATE TABLE t1 (c1 INT,	c2 VARCHAR(100), c3 INT,	c4 INT,	c5 INT,	PRIMARY KEY(c1,c2,c4,c5),	KEY(c1) LOCAL) PARTITION BY $part_type (c1,c2,c4,c5) PARTITIONS 4;

eval CREATE TABLE t2 (c1 INT,	c2 VARCHAR(100), c3 INT,	c4 INT,	c5 INT,	PRIMARY KEY(c1,c2,c4,c5),	KEY(c1) LOCAL) PARTITION BY $part_type (c1,c2,c4,c5) PARTITIONS 4;


#Insert data
let $cnt = 30;
#--disable_query_log
while ($cnt)
{
	eval REPLACE INTO t1 VALUES($cnt, 'C$cnt', $cnt * $cnt, $cnt, $cnt);
	eval INSERT  INTO t1 VALUES($cnt + 10000, 'C$cnt', $cnt * $cnt + 10000, $cnt, $cnt);
	dec $cnt;
}
let $cnt = 60;
#--disable_query_log
while ($cnt > 30)
{
	eval REPLACE INTO t1 VALUES($cnt, 'C$cnt', $cnt * $cnt, $cnt, $cnt);
	eval INSERT  INTO t1 VALUES($cnt + 10000, 'C$cnt', $cnt * $cnt + 10000, $cnt, $cnt);
	dec $cnt;
}
#--enable_query_log

# In the following sections, needs to cover test of select, update, delete, join, union

# - select
select * from t1 order by c1,c2,c4,c5;
select * from t1 partition(p0) order by c1,c2,c4,c5;
select * from t1 partition(p1) order by c1,c2,c4,c5;
select * from t1 partition(p2) order by c1,c2,c4,c5;
select * from t1 partition(p3) order by c1,c2,c4,c5;

explain  select * from t1 where c1 = 3;
select * from t1 where c1 = 3;

explain  select * from t1 where c1 <=> 3;
select * from t1 where c1 <=> 3;

explain  select * from t1 where c1 = 3 or c1 = 4;
select * from t1 where c1 = 3 or c1 = 4;

explain  select * from t1 where c1 > 98;
select * from t1 where c1 > 98 order by c1,c2,c4,c5;

explain  select * from t1 where c1 >=98;
select * from t1 where c1 >=98 order by c1,c2,c4,c5;

explain  select * from t1 where c1 < 3;
select * from t1 where c1 < 3;

explain  select * from t1 where c1 <= 3;
select * from t1 where c1 <= 3;

explain  select * from t1 where c1>=1 and c1<=3;
select * from t1 where c1>=1 and c1<=3;


explain  select * from t1 where c4 = 3;
select * from t1 where c4 = 3;

explain  select * from t1 where c4 <=> 3;
select * from t1 where c4 <=> 3;

explain  select * from t1 where c4 = 3 or c4 = 4;
select * from t1 where c4 = 3 or c4 = 4;

explain  select * from t1 where c4 > 98;
select * from t1 where c4 > 98 order by c1,c2,c4,c5;

explain  select * from t1 where c4 >=98;
select * from t1 where c4 >=98 order by c1,c2,c4,c5;

explain  select * from t1 where c4 < 3;
select * from t1 where c4 < 3;

explain  select * from t1 where c4 <= 3;
select * from t1 where c4 <= 3;

explain  select * from t1 where c4>=1 and c4<=3;
select * from t1 where c4>=1 and c4<=3;


explain  select * from t1 where c5 = 3;
select * from t1 where c5 = 3;

explain  select * from t1 where c5 <=> 3;
select * from t1 where c5 <=> 3;

explain  select * from t1 where c5 = 3 or c5 = 4;
select * from t1 where c5 = 3 or c5 = 4;

explain  select * from t1 where c5 > 98;
select * from t1 where c5 > 98 order by c1,c2,c4,c5;

explain  select * from t1 where c5 >=98;
select * from t1 where c5 >=98 order by c1,c2,c4,c5;

explain  select * from t1 where c5 < 3;
select * from t1 where c5 < 3;

explain  select * from t1 where c5 <= 3;
select * from t1 where c5 <= 3;

explain  select * from t1 where c5>=1 and c5<=3;
select * from t1 where c5>=1 and c5<=3;

# no pruning
explain  select * from t1 where c2='C10';
select * from t1 where c2='C10';


#explain  select * from t1 partition(p0);
select * from t1 partition(p0);
select * from t1 partition(p0,p1);
select * from t1;

# - update
explain  update t1 set c2=concat(c2,'_up') where c1=1;
update t1 set c2=concat(c2,'_up') where c1=1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c1<=>1;
update t1 set c2=concat(c2,'_up') where c1<=>1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c1=2 or c3=3;
update t1 set c2=concat(c2,'_up') where c1=2 or c3=3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c1>98;
update t1 set c2=concat(c2,'_up') where c1>98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c1>=98;
update t1 set c2=concat(c2,'_up') where c1>=98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c1<3;
update t1 set c2=concat(c2,'_up') where c1<3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c1<=3;
update t1 set c2=concat(c2,'_up') where c1<=3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c1<=3 and c1>=1;
update t1 set c2=concat(c2,'_up') where c1<=3 and c1>=1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log


explain  update t1 set c2=concat(c2,'_up') where c4=1;
update t1 set c2=concat(c2,'_up') where c4=1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c4<=>1;
update t1 set c2=concat(c2,'_up') where c4<=>1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c4=2 or c3=3;
update t1 set c2=concat(c2,'_up') where c4=2 or c3=3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c4>98;
update t1 set c2=concat(c2,'_up') where c4>98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c4>=98;
update t1 set c2=concat(c2,'_up') where c4>=98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c4<3;
update t1 set c2=concat(c2,'_up') where c4<3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c4<=3;
update t1 set c2=concat(c2,'_up') where c4<=3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c4<=3 and c4>=1;
update t1 set c2=concat(c2,'_up') where c4<=3 and c4>=1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log


explain  update t1 set c2=concat(c2,'_up') where c5=1;
update t1 set c2=concat(c2,'_up') where c5=1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c5<=>1;
update t1 set c2=concat(c2,'_up') where c5<=>1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c5=2 or c3=3;
update t1 set c2=concat(c2,'_up') where c5=2 or c3=3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c5>98;
update t1 set c2=concat(c2,'_up') where c5>98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c5>=98;
update t1 set c2=concat(c2,'_up') where c5>=98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c5<3;
update t1 set c2=concat(c2,'_up') where c5<3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c5<=3;
update t1 set c2=concat(c2,'_up') where c5<=3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c5<=3 and c5>=1;
update t1 set c2=concat(c2,'_up') where c5<=3 and c5>=1;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

select * from t1 order by c1,c2,c4,c5;

# no pruning
explain  update t1 set c2=concat(c2,'_up') where c2='C10';
update t1 set c2=concat(c2,'_up') where c2='C10';
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  update t1 set c2=concat(c2,'_up') where c2>'C90';
update t1 set c2=concat(c2,'_up') where c2>'C90';
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

select * from t1 order by c1,c2,c4,c5;

# - delete
explain  delete from t1 where c1 = 3;
delete from t1 where c1 = 3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c1 <=> 4;
delete from t1 where c1 <=> 4;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c1 = 5 or c1 = 6;
delete from t1 where c1 = 5 or c1 = 6;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c1 >= 7 and c1 <= 8;
delete from t1 where c1 >= 7 and c1 <= 8;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

select * from t1 order by c1,c2,c4,c5;


explain  delete from t1 where c4 = 13;
delete from t1 where c4 = 13;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c4 <=> 14;
delete from t1 where c4 <=> 14;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c4 = 15 or c4 = 16;
delete from t1 where c4 = 15 or c4 = 16;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c4 >= 17 and c4 <= 18;
delete from t1 where c4 >= 17 and c4 <= 18;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log


explain  delete from t1 where c5 = 23;
delete from t1 where c5 = 23;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c5 <=> 24;
delete from t1 where c5 <=> 24;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c5 = 25 or c5 = 26;
delete from t1 where c5 = 25 or c5 = 26;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c5 >= 27 and c5 <= 28;
delete from t1 where c5 >= 27 and c5 <= 28;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log


explain  delete from t1 where c1 > 98;
delete from t1 where c1 > 98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c1 >= 98;
delete from t1 where c1 >= 98;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c1<3;
delete from t1 where c1<3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

explain  delete from t1 where c1<=3;
delete from t1 where c1<=3;
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

select * from t1 order by c1,c2,c4,c5;

# no pruning
explain  delete from t1 where c2='C10';
delete from t1 where c2='C10';
--disable_query_log
--disable_result_log
select count(*) from t1;
--enable_result_log
--enable_query_log

#explain  delete from t1 partition(p0);
delete from t1 partition(p0);
delete from t1 partition(p0,p1);
delete from t1;

select * from t1 order by c1,c2,c4,c5;

# - join
explain  select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t2.c1<15 order by c1,c2,c4,c5;
select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t2.c1<15 order by c1,c2,c4,c5;

explain  select * from t1 join t2 on t1.c2=t2.c2 where t1.c4 < 10 and t2.c4<15 order by c1,c2,c4,c5;
select * from t1 join t2 on t1.c2=t2.c2 where t1.c4 < 10 and t2.c4<15 order by c1,c2,c4,c5;

explain  select * from t1 join t2 on t1.c2=t2.c2 where t1.c5 < 10 and t2.c5<15 order by c1,c2,c4,c5;
select * from t1 join t2 on t1.c2=t2.c2 where t1.c5 < 10 and t2.c5<15 order by c1,c2,c4,c5;

explain  select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t1.c1>=8 and t2.c1<15 order by c1,c2,c4,c5;
select * from t1 join t2 on t1.c2=t2.c2 where t1.c1 < 10 and t1.c1>=8 and t2.c1<15 order by c1,c2,c4,c5;

# no pruning
explain  select * from t1 join t2 on t1.c2=t2.c2 where t1.c2 = 'C10' order by c1,c2,c4,c5;
select * from t1 join t2 on t1.c2=t2.c2 where t1.c2 = 'C10' order by c1,c2,c4,c5;


# - union
explain  ( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c2='C1' ) order by c1,c2,c4,c5;
( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c2='C1' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1>10 ) union ( select * from t2 where c1>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c1>10 ) union ( select * from t2 where c1>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c2<=>'C1' ) order by c1,c2,c4,c5;
( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c2<=>'C1' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1=1 ) union all ( select * from t2 where c1>1 and c2='C1' ) order by c1,c2,c4,c5;
( select * from t1 where c1=1 ) union all ( select * from t2 where c1>1 and c2='C1' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1>10 ) union all ( select * from t2 where c1>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c1>10 ) union all ( select * from t2 where c1>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1<=>1 ) union all ( select * from t2 where c1>1 and c2<=>'C1' ) order by c1,c2,c4,c5;
( select * from t1 where c1<=>1 ) union all ( select * from t2 where c1>1 and c2<=>'C1' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c2='C1' ) order by c1,c2,c4,c5;
( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c2='C1' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1>10 ) union ( select * from t2 where c1>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c1>10 ) union ( select * from t2 where c1>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c2<=>'C1' ) order by c1,c2,c4,c5;
( select * from t1 where c1<=>1 ) union ( select * from t2 where c1>1 and c2<=>'C1' ) order by c1,c2,c4,c5;


explain  ( select * from t1 where c4=1 ) union ( select * from t2 where c4>1 and c2='c4' ) order by c1,c2,c4,c5;
( select * from t1 where c4=1 ) union ( select * from t2 where c4>1 and c2='c4' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4>10 ) union ( select * from t2 where c4>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c4>10 ) union ( select * from t2 where c4>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4<=>1 ) union ( select * from t2 where c4>1 and c2<=>'c4' ) order by c1,c2,c4,c5;
( select * from t1 where c4<=>1 ) union ( select * from t2 where c4>1 and c2<=>'c4' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4=1 ) union all ( select * from t2 where c4>1 and c2='c4' ) order by c1,c2,c4,c5;
( select * from t1 where c4=1 ) union all ( select * from t2 where c4>1 and c2='c4' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4>10 ) union all ( select * from t2 where c4>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c4>10 ) union all ( select * from t2 where c4>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4<=>1 ) union all ( select * from t2 where c4>1 and c2<=>'c4' ) order by c1,c2,c4,c5;
( select * from t1 where c4<=>1 ) union all ( select * from t2 where c4>1 and c2<=>'c4' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4=1 ) union ( select * from t2 where c4>1 and c2='c4' ) order by c1,c2,c4,c5;
( select * from t1 where c4=1 ) union ( select * from t2 where c4>1 and c2='c4' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4>10 ) union ( select * from t2 where c4>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c4>10 ) union ( select * from t2 where c4>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c4<=>1 ) union ( select * from t2 where c4>1 and c2<=>'c4' ) order by c1,c2,c4,c5;
( select * from t1 where c4<=>1 ) union ( select * from t2 where c4>1 and c2<=>'c4' ) order by c1,c2,c4,c5;


explain  ( select * from t1 where c5=1 ) union ( select * from t2 where c5>1 and c2='c5' ) order by c1,c2,c4,c5;
( select * from t1 where c5=1 ) union ( select * from t2 where c5>1 and c2='c5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5>10 ) union ( select * from t2 where c5>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c5>10 ) union ( select * from t2 where c5>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5<=>1 ) union ( select * from t2 where c5>1 and c2<=>'c5' ) order by c1,c2,c4,c5;
( select * from t1 where c5<=>1 ) union ( select * from t2 where c5>1 and c2<=>'c5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5=1 ) union all ( select * from t2 where c5>1 and c2='c5' ) order by c1,c2,c4,c5;
( select * from t1 where c5=1 ) union all ( select * from t2 where c5>1 and c2='c5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5>10 ) union all ( select * from t2 where c5>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c5>10 ) union all ( select * from t2 where c5>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5<=>1 ) union all ( select * from t2 where c5>1 and c2<=>'c5' ) order by c1,c2,c4,c5;
( select * from t1 where c5<=>1 ) union all ( select * from t2 where c5>1 and c2<=>'c5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5=1 ) union ( select * from t2 where c5>1 and c2='c5' ) order by c1,c2,c4,c5;
( select * from t1 where c5=1 ) union ( select * from t2 where c5>1 and c2='c5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5>10 ) union ( select * from t2 where c5>1 and c2<'C5' ) order by c1,c2,c4,c5;
( select * from t1 where c5>10 ) union ( select * from t2 where c5>1 and c2<'C5' ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c5<=>1 ) union ( select * from t2 where c5>1 and c2<=>'c5' ) order by c1,c2,c4,c5;
( select * from t1 where c5<=>1 ) union ( select * from t2 where c5>1 and c2<=>'c5' ) order by c1,c2,c4,c5;


explain  ( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c1<=4 ) order by c1,c2,c4,c5;
( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c1<=4 ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1=1 ) union all ( select * from t2 where c1>1 and c1<=4 ) order by c1,c2,c4,c5;
( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c1<=4 ) order by c1,c2,c4,c5;

explain  ( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c1<=4 ) order by c1,c2,c4,c5;
( select * from t1 where c1=1 ) union ( select * from t2 where c1>1 and c1<=4 ) order by c1,c2,c4,c5;

# no pruning
explain  ( select * from t1 where c2='C15' ) union ( select * from t2 where c2='C16' ) order by c1,c2,c4,c5;
( select * from t1 where c2='C15' ) union ( select * from t2 where c2='C16' ) order by c1,c2,c4,c5;


