
#########  SELECT
#** for cast
select * from type_t where binary_c = cast(100 as binary (1));
select * from type_t where binary_c = cast(100 as binary (2));
select * from type_t where binary_c = cast(100 as binary);
select * from type_t where char_c = cast(100 as char(1));
select * from type_t where char_c = cast(100 as char(2));
select * from type_t where char_c = cast(100 as char);
select * from type_t where datetime_c = cast(100 as datetime(1));
select * from type_t where datetime_c = cast(100 as datetime(2));
select * from type_t where datetime_c = cast(100 as datetime);
select * from type_t where numeric_c = cast(100 as number(10, 1));
select * from type_t where numeric_c = cast(100 as number(9, 1));
select * from type_t where numeric_c = cast(100 as number(9));
select * from type_t where numeric_c = cast(100 as number);
select * from type_t where int_c = cast(100 as signed);
select * from type_t where int_c = cast(100 as unsigned);

#** basic
select 0;
#col name is null in mysql and NULL in OB
#select null;
select '123';

select * from type_t where datetime_c = FROM_UNIXTIME(UNIX_TIMESTAMP('2015-01-01 01:01:01'), '%Y-%m-%d');
select * from type_t where datetime_c = FROM_UNIXTIME(UNIX_TIMESTAMP('2015-01-01 01:01:01'), '%Y-%m-%d %H:%i:%s');
select * from type_t where datetime_c = FROM_UNIXTIME(UNIX_TIMESTAMP('2015-01-01 00:00:00'));
select * from type_t where datetime_c = FROM_UNIXTIME(1420041600);
select 1 from dual where FROM_UNIXTIME(1477065599 , '%Y-%m-%d') > timestamp'2016-10-21 00:00:00';
select 1 from dual where FROM_UNIXTIME(1477065599 ,substr('%Y-%m-%d %H:%i:%s',1,8)) > timestamp'2016-10-21 00:00:00';
select 1 from dual where str_to_date('20161021' ,substr('%Y%m%d%H%i%s',1,6)) > timestamp'2016-10-21 00:00:00';
select 1 from dual where str_to_date('20161021' ,'%Y%m%d') > timestamp'2016-10-21 00:00:00';
select 1 from dual where CAST((ROUND(10.2345, 0)) AS CHAR(100)) = '10';
select * from type_t where int_c = 1;
select * from type_t where tinyint_c = 1;
select * from type_t where bool_c = true;
select * from type_t where smallint_c = 1;
select * from type_t where mediumint_c = 1;
select * from type_t where integer_c = 1;
select * from type_t where bigint_c = 1;
select * from type_t where float_c = 0.1;
select * from type_t where double_c = 0.1;
select * from type_t where decimal_c = 0.100;
select * from type_t where numeric_c = 0.100;
select * from type_t where char_c = 'good';
select * from type_t where varchar_c = 'good';
select * from type_t where binary_c = 'good';
select * from type_t where date_c = '2015-01-01';
select * from type_t where datetime_c = '2015-01-01 01:01:01';
select * from type_t where time_c = '01:01:01';
select * from type_t where year_c = 70;

#** function
select * from type_t where datetime_c = now();
select datetime_c from type_t where datetime_c = now(0);
select datetime_c, time_c from type_t where datetime_c = now(1);
select * from type_t where datetime_c = now(1);
select * from type_t where time_c = current_time();
select * from type_t where time_c = current_time(1);
select * from type_t where datetime_c = current_timestamp();
select * from type_t where datetime_c = current_timestamp(1);
select * from type_t where time_c = curtime();
select * from type_t where time_c = curtime(1);
select * from type_t where date_c = current_date;
select * from type_t where date_c = current_date();
select * from type_t where date_c = date_add('2015-01-01', interval 5 day);
select * from type_t where varchar_c = date_format('2015-01-01', '%Y-%m-%d');
select * from type_t where datetime_c = date_format('2015-01-01 00:00:00', '%Y-%m-%d');
select * from type_t where datetime_c = date_format('2015-01-01 00:00:00', '%Y-%m-%d %H:%i:%s');
select * from type_t where date_c = date_sub('2015-01-06', interval 5 day);
#mysql has warning but ob do not
#select * from type_t where varchar_c = extract(week from '2015-01-12');
select * from type_t where datetime_c = str_to_date('2015-01-01', '%Y-%m-%d');
#mysql not support time_to_usec
#select * from type_t where bigint_c = time_to_usec('2015-01-01') - time_to_usec('2015-01-01');
#mysql not support usec_to_time
#select * from type_t where datetime_c = usec_to_time(1);
select * from type_t where bigint_c = unix_timestamp('1997-01-01 08:00:10');
select * from type_t where bigint_c = datediff('2015-01-08', '2015-01-04');
select * from type_t where bigint_c = timestampdiff(day, '2015-01-01', '2015-01-05');
select * from type_t where bigint_c = period_diff(20151001, 20150901);

select * from type_t where varchar_c = concat('hello', 'world');
select * from type_t where varchar_c = concat('hello', NULL);
select * from type_t where varchar_c = substr('ahello', 2);
select * from type_t where varchar_c = substr('abhello', 3, 5);
select * from type_t where varchar_c = substr('agood' from -4 for 4);
#for trim
select * from type_t where varchar_c = trim(' hello ');
select * from type_t where varchar_c = trim(leading 'x' from 'xxhelloworldxx');
select * from type_t where varchar_c = trim(leading from '  hello  ');
select * from type_t where varchar_c = trim(trailing 'x' from 'xxhelloworldxx');
select * from type_t where varchar_c = trim(trailing from '  hello  ');
select * from type_t where varchar_c = trim(both 'x' from 'xxhelloworldxx');
select * from type_t where varchar_c = trim(both from '  hello  ');

select * from type_t where int_c = length('helloworld');
select * from type_t where int_c = length(-1.23);
select * from type_t where varchar_c = upper('hello');
select * from type_t where varchar_c = lower('HELLO');
select * from type_t where varchar_c = hex(255);
select * from type_t where varchar_c = hex(0x012);
select * from type_t where varchar_c = unhex(hex('helloworld'));
#mysql not support int2ip
#select * from type_t where varchar_c = int2ip(1);
#mysql not support ip2int
#select * from type_t where int_c = ip2int('0.0.0.1');
select * from type_t where bool_c = 'abc' like '%bc';
select * from type_t where bool_c = 'abc' not like '%bc';
select * from type_t where bool_c = 'ab%' like 'abc%' escape 'c';
select * from type_t where bool_c = 1234 regexp 1;
select * from type_t where bool_c = 'hello' regexp 'h';
select * from type_t where bool_c = 'hello' rlike 'h%';

select * from type_t where varchar_c = repeat('1',4);
select * from type_t where varchar_c = repeat(1,4);

select * from type_t where varchar_c = substring_index('helloabcd', 'abcd', 1);
select * from type_t where int_c = locate('bar', 'foobarbar');
select * from type_t where int_c = locate('bar', 'foobarbar', 5);
select * from type_t where int_c = instr('foobarbar', 'bar');
select * from type_t where varchar_c = replace('xxxloworld', 'xxx', 'hel');
select * from type_t where int_c = field('aa', 'bb','cc', 'aa');

# mysql :Warning	1292	Truncated incorrect CHAR(10) value: 'helloworldxxx'
#select * from type_t where varchar_c = cast('helloxx' as char(5));

select * from type_t where int_c = round(1.1);
select * from type_t where double_c = round(111.1e-2,1);
select * from type_t where int_c = ceil(0.95);
select * from type_t where int_c = floor(1.1);
select * from type_t where int_c = abs(-1);
#mysql not support neg
#select * from type_t where int_c = neg(-1);
select * from type_t where int_c = sign(-2);
select * from type_t where varchar_c = conv(1111, 2, 10);

select * from type_t where int_c = greatest(2, '1', 0);
select * from type_t where int_c = greatest(2, 1, 0);
select * from type_t where int_c = least(2, '1', 0);
select * from type_t where int_c = least(1, 1, 0);

select c1, case c1 when 1 then 'is 1' else 'not 1' end from t1 where c1 = 1;
# k3:r6538287
select c1 from t1 where c1 = 1;

#**operator
select * from t1 where c1 = ((2 + 2 -2)*2/2)%10;
select * from t1 where c1 = (2 > 3) or (2 > 3) or (2 > 3);
select * from t1 where c1 = ((2 < 4) and (3 > 4)) or (4 > 5);
select * from t1 where c1 = (c1 >= 2 and c2 <= 2) or c1 = 1;
select * from t1 where (c1, c2) = (1, 1);
select * from t1 where c1 <=> 1;

#bug r6565812
#select * from dist_t where c1 = ((2 + 2 -2)*2/2)%10;
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 = ((2 < 4) and (3 > 4)) or (4 > 5);
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 = (c1 >= 2 and c2 <= 2) or c1 = 1;
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 <=> 1;

select * from t1 where c1 not between 1 and 2;
select * from t1 where c1 between 0 and 3;
select * from t1 where c1 between null and 3;

select/*+ read_consistency(WEAK)*/ * from dist_t where c1 between 0 and 3;

select * from t1 where c1 in (1, 2);
select * from t1 where (c1, c2) in ((2,1),(5,3));

select/*+ read_consistency(WEAK)*/ * from dist_t where (c1, c2) in ((2,1),(5,3));

select * from t1 where c1 = (null is null);
select * from t1 where c1 = (null is true);
select * from t1 where c1 = ((0>1) is false);
select * from t1 where c1 = (null is unknown);
select * from t1 where c1 = (0 is not null);
select * from t1 where c1 = (null is not null);
select * from t1 where c1 = (null is not true);
select * from t1 where c1 = ((0>1) is not false);
select * from t1 where c1 = (null is not unknown);

select * from t1 where c1 = 5 & 1;
select * from t1 where c1 = 5 | 5;
select * from t1 where c1 = 5 ^ 5;

select * from t1 where exists(select * from t2 where c1 = 1);
select * from t2 where c1 = 1 and c2 = 1 and c3 like '%test%' and c1 between 1 and 10;

#***as
select c1 c1, c2 m from t1 where c1 = 1 or c2 = 2 or c1 < 5;
select s.c1, s.c2+1 from (select c1, c2 from t1) s;
select j1.c1 from t1 as j1, t1 as j2 where j1.c1 = j2.c2 and j1.c1 = 1;
select * from (select * from t1) s;

select/*+ read_consistency(WEAK)*/ j1.c1 from dist_t as j1, dist_t as j2 where j1.c1 = j2.c2 and j1.c1 = 1;

#**group by
select c1, sum(c2), avg(c2), count(c2), max(c2), min(c2) from t1 group by c1;
select c1, sum(c2) from t1 group by c1+2 order by c1+1;
select c1, sum(c2) from t1 group by c1 having c1 > 1;
select c2+1 as c5, sum(c1) from t1 group by c5;
select sum(c1)+sum(c2) from t2 group by c2 having sum(c1) > 7 order by 1;
#result sort is different
#select/*+ read_consistency(WEAK)*/ sum(c1)+sum(c2) from dist_t group by c2 having sum(c1) > 7;

#** having
select c1, sum(c2) from t1 group by c1 having sum(c2) > 4;
select * from t1 having c1 = 1;
#remote plan
select/*+ read_consistency(WEAK)*/ * from dist_t having c1 = 2;

#** order by
select * from t1 where c1 = 1 order by c2;
select * from t1 order by 2;
SELECT c1 FROM t1 ORDER BY SUM(c1) + 1;
SELECT c1 FROM t1 ORDER BY SUM(c1 + 1);
select * FROM t1  ORDER BY c1 desc LIMIT 3;
select c1, sum(c2) from t1 group by c1 order by 1 desc, 2 asc;

select/*+ read_consistency(WEAK)*/ c1, sum(c2) from dist_t group by c1 order by 1 desc, 2 asc LIMIT 3;

#** limit
select * from t1 limit 1;
select c1, c2 from t1 limit 2, 6;

#** join
SELECT * FROM t1 INNER JOIN t2 on t1.c1=t2.c1 order by t1.c1;
select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end order by t1.c1;
SELECT * from t1 JOIN t2 on t1.c1 = t2.c1 order by t1.c1;
SELECT * from t1 CROSS JOIN t2 on t1.c1=t2.c1 order by t1.c1;
SELECT * from t1 LEFT JOIN t2 on t1.c1=t2.c1 order by t1.c1;
#sort diff
#SELECT * FROM t1 LEFT JOIN t2 ON t1.c1=t2.c2 WHERE not(0+(t1.c1=6 and t2.c2=1)) order by t1.c1;
SELECT * from t1 RIGHT JOIN t2 on t1.c1=t2.c1 order by t2.c1;

#sort diff
#SELECT/*+ read_consistency(WEAK)*/ * from dist_t JOIN t2 on dist_t.c1 = t2.c1 order by dist_t.c1;

#** union
select * from t1 union select * from t1 union select * from t1;
select * from t1 union all select * from t1;

#** except
#mysql not support
#select c1 from t1 except select c1 from t2 where c1 = 1;

#mysql not support
#select/*+ read_consistency(WEAK)*/ c1 from t1 except select c1 from dist_t where c1 = 1;

#** intersect
#mysql not suport
#select c1 from t1 intersect select c1 from t2 where c1 = 1;

#mysql not suport
#select/*+ read_consistency(WEAK)*/ c1 from t1 intersect select c1 from dist_t where c1 = 1 or c1 = 2;

#** for update
select * from t1 for update nowait;
select c1 from t1 where c1 = 1 for update;
#mysql not support
#select c1 from t1 where c1 = 1 for update wait 1;

#mysql not support
#select/*+ read_consistency(WEAK)*/ c1 from dist_t where c1 = 2 for update wait 1;

#** subquery
select * from t1 where t1.c1 > any(select t2.c2 from t2);
select c1, c2, c1 in (select c2 from t1), c1 = all(select c1 from t1) from t1;
select t1.c1 from t1, (select * from t2) as v order by t1.c1;
select * from t1, t2 where t1.c1 > exists(select c2 from t2 where t2.c1 = t1.c1);
#ob not support dist
#select/*+ read_consistency(WEAK)*/ * from t1, t2 where t1.c1 > exists(select c2 from dist_t where dist_t.c1 = t1.c1);

select * from t1 where (1 = 1) or c1 = 1;
select * from t1 where 1 or c1 = 1;
select c1, c2 from t1 group by c1 having (1 = 1 or c1 > 5);
select * from t1 join t2 on (1 = 1 or t1.c1 = t2.c1) order by t1.c1;
select * from t1 where c1 in (select c1 from t2 where (1 = 0 or c1 > 5));

select/*+QUERY_TIMEOUT(100000000)*/c1 from t1 where c1 > 5;
select/*+QUERY_TIMEOUT(100000000)*/c1 from t1 where c1 = 5;

select * from t2 where c3 like '%he%' escape NULL;
select * from t2 where c3 not like '%he%' escape NULL;

#########  DELETE
delete from t1 where c1 = 10;
delete from t1 where c1 > 10;
#delete from t1;

#ob not support
#delete from dist_t where c1 > 10;

#########  UPDATE
update t1 set c2 = 1 where c1 = 1;
update t1 t set t.c2 = 1 where c1 = 1;
update t2 set c2 = c2 + 100, c3 = 'test' where c1 = 1;
update t2 set c2 = c3 like '%h%' where c1 = 1;
update t2 set c2 = 10 where c2 = 1;

#ob not support
#update dist_t set c2 = 10 where c1 = 2 or c1 = 3;

#########  INSERT
truncate table t2;
insert into t2 values(1, 1, 'test');
insert into t2(c1, c2, c3) values(3, 1, 'test');
insert into t2(c1, c2, c3) values(5, 1, 'test'), (7, 2, 'hello'), (10, 3, 'world');

#values 后面有空格,所以不匹配上面的insert into t2 values(1, 1, 'test');
insert into t2 values (20, 20, null);
insert into t2 values(24, null+null, null);
insert into t2 values(26, 1, 1+null), (28, null, null);
##expect 0, because 1+null and null + 1 need mache type
insert into t2 values(30, 1, 1+null);

replace into t2 values(35, 1, NULL),(36, 1, 'good');

#delete from dist_t where c1 > 10;
#ob not support
#insert into dist_t values(13, 13), (14,14);

#########  REPLACE
replace into t1 values(1, null);
replace into t1 values(13, null), (15, 1+1);

#ob not support
#replace into dist_t values(18, 9999), (19, 999);

### acs case
select * from acs_test where c2 = 1;
