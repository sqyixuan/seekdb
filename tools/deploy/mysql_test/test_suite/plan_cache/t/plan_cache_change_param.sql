
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
select 1;
#col name is null in mysql and NULL in OB
#select null;
select '1234';

select * from type_t where datetime_c = FROM_UNIXTIME(UNIX_TIMESTAMP('2015-01-01 01:01:01'), '%Y-%m-%d');
select * from type_t where datetime_c = FROM_UNIXTIME(UNIX_TIMESTAMP('2015-01-01 01:01:01'), '%Y-%m-%d %H:%i:%s');
select * from type_t where datetime_c = FROM_UNIXTIME(UNIX_TIMESTAMP('2015-01-01 01:01:01'));
select * from type_t where datetime_c = FROM_UNIXTIME(1420045261);
select 1 from dual where FROM_UNIXTIME(1477065599 ,'%Y-%m-%d %H:%i:%s') > timestamp'2016-10-21 00:00:00';
select 1 from dual where FROM_UNIXTIME(1477065599 ,substr('%Y-%m-%d %H:%i:%s',1,11)) > timestamp'2016-10-21 00:00:00';
select 1 from dual where str_to_date('20161021235959' ,substr('%Y%m%d%H%i%s',1,8)) > timestamp'2016-10-21 00:00:00';
select 1 from dual where str_to_date('20161021235959' ,'%Y%m%d%H%i%s') > timestamp'2016-10-21 00:00:00';
select 1 from dual where CAST((ROUND(10.2345, 2)) AS CHAR(100)) = '10';
select * from type_t where int_c = 2;
select * from type_t where tinyint_c = 2;
select * from type_t where bool_c = false;
select * from type_t where smallint_c = 2;
select * from type_t where mediumint_c = 2;
select * from type_t where integer_c = 2;
select * from type_t where bigint_c = 2;
select * from type_t where float_c = 0.2;
select * from type_t where double_c = 0.2;
select * from type_t where decimal_c = 0.110;
select * from type_t where numeric_c = 0.110;
select * from type_t where char_c = 'goodman';
select * from type_t where varchar_c = 'goodman';
select * from type_t where binary_c = 'goodman';
select * from type_t where date_c = '2015-02-01';
select * from type_t where datetime_c = '2015-01-01 02:02:02';
select * from type_t where time_c = '02:02:02';
select * from type_t where year_c = 1980;

#** function
select * from type_t where datetime_c = now();
select datetime_c from type_t where datetime_c = now(0);
select datetime_c, time_c from type_t where datetime_c = now(1);
select * from type_t where datetime_c = now(2);
select * from type_t where time_c = current_time();
select * from type_t where time_c = current_time(2);
select * from type_t where datetime_c = current_timestamp();
select * from type_t where datetime_c = current_timestamp(2);
select * from type_t where time_c = curtime();
select * from type_t where time_c = curtime(2);
select * from type_t where date_c = current_date;
select * from type_t where date_c = current_date();
select * from type_t where date_c = date_add('2015-01-02', interval 4 day);
select * from type_t where varchar_c = date_format('2015-01-06', '%Y-%m-%d');
select * from type_t where datetime_c = date_format('2015-01-01 01:01:01', '%Y-%m-%d');
select * from type_t where datetime_c = date_format('2015-01-01 00:00:00', '%Y-%m-%d %H:%i:%s');
select * from type_t where date_c = date_sub('2015-01-07', interval 6 day);
#mysql has warning but ob do not
#select * from type_t where varchar_c = extract(week from '2015-01-05');
select * from type_t where datetime_c = str_to_date('2015-11-11', '%Y-%m-%d');
#mysql not support time_to_usec
#select * from type_t where bigint_c = time_to_usec('2015-11-11') - time_to_usec('2015-11-11');
#mysql not support usec_to_time
#select * from type_t where datetime_c = usec_to_time(2);
select * from type_t where bigint_c = unix_timestamp('1997-01-01 08:00:00');
select * from type_t where bigint_c = datediff('2015-01-04', '2015-01-08');
select * from type_t where bigint_c = timestampdiff(day, '2015-01-10', '2015-01-06');
select * from type_t where bigint_c = period_diff(20151011, 20150911);

select * from type_t where varchar_c = concat('hello', 'everyone');
select * from type_t where varchar_c = concat('hello world', NULL);
select * from type_t where varchar_c = substr('abhello', 3);
select * from type_t where varchar_c = substr('ahello', 2, 5);
select * from type_t where varchar_c = substr('abhello' from -5 for 5);
#for trim
select * from type_t where varchar_c = trim(' helloworld ');
select * from type_t where varchar_c = trim(leading 'y' from 'yyyhelloyyy');
select * from type_t where varchar_c = trim(leading from '  hello  ');
select * from type_t where varchar_c = trim(trailing 'y' from 'yyyhelloyyy');
select * from type_t where varchar_c = trim(trailing from '  hello  ');
select * from type_t where varchar_c = trim(both 'y' from 'yyyhelloyyy');
select * from type_t where varchar_c = trim(both from '  hello  ');

select * from type_t where int_c = length('hello');
select * from type_t where int_c = length(-1.235);
select * from type_t where varchar_c = upper('helloworld');
select * from type_t where varchar_c = lower('HELLOWORLD');
select * from type_t where varchar_c = hex(128);
select * from type_t where varchar_c = hex(0x0123);
select * from type_t where varchar_c = unhex(hex('hello'));
#mysql not support int2ip
#select * from type_t where varchar_c = int2ip(21);
#mysql not support ip2int
#select * from type_t where int_c = ip2int('0.0.0.21');
select * from type_t where bool_c = 'aabc' like '%abc';
select * from type_t where bool_c = 'abcd' not like '%bc%';
select * from type_t where bool_c = 'aab%' like 'aabc%' escape 'c';
select * from type_t where bool_c = 123 regexp 2;
select * from type_t where bool_c = 'helloworld' regexp 'he';
select * from type_t where bool_c = 'helloworld' rlike 'he%';

select * from type_t where varchar_c = repeat('11',2);
select * from type_t where varchar_c = repeat(11,2);

select * from type_t where varchar_c = substring_index('helloabc', 'abc', 1);
select * from type_t where int_c = locate('barb', 'foobarbarbar');
select * from type_t where int_c = locate('barb', 'foobarbarbar', 5);
select * from type_t where int_c = instr('foobarbar', 'bar');
select * from type_t where varchar_c = replace('xxxxoworld', 'xxxx', 'hell');
select * from type_t where int_c = field('aaa', 'bbb','ccc', 'aaa');

# mysql :Warning	1292	Truncated incorrect CHAR(10) value: 'helloworldxxx'
#select * from type_t where varchar_c = cast('helloworldxxx' as char(10));

select * from type_t where int_c = round(1.5);
select * from type_t where double_c = round(11.11e-2,2);
select * from type_t where int_c = ceil(0.87);
select * from type_t where int_c = floor(1.11);
select * from type_t where int_c = abs(2);
#mysql not support neg
#select * from type_t where int_c = neg(2);
select * from type_t where int_c = sign(0);
select * from type_t where varchar_c = conv(15, 10, 2);

select * from type_t where int_c = greatest(5, '6', 0);
select * from type_t where int_c = greatest(1, 8, 10);
select * from type_t where int_c = least(20, '11', 0);
select * from type_t where int_c = least(100, 11, 0);

select c1, case c1 when 0 then '0' else 'no 0' end from t1 where c1 = 0;
# k3:r6538287
select c1 from t1 where c1 = '1';

#**operator
select * from t1 where c1 = ((20 + 20 -20)*20/20)%20;
select * from t1 where c1 = (20 > 30) or (20 > 30) or (20 > -3);
select * from t1 where c1 = ((-2 < -4) and (33 > 4)) or (44 > 5);
select * from t1 where c1 = (c1 >= 20 and c2 <= 5) or c1 = 11;
select * from t1 where (c1, c2) = (2, 2);
select * from t1 where c1 <=> 10;

#bug r6565812
#select/*+ read_consistency(WEAK)*/ * from dist_t where c1 = ((2 + 2 -2)*2/2)%10;
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 = ((2 < 4) and (3 > 4)) or (4 > 5);
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 = (c1 >= 2 and c2 <= 2) or c1 = 1;
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 <=> 1;

select * from t1 where c1 not between 5 and 10;
select * from t1 where c1 between 3 and 5;
select * from t1 where c1 between null and 30;

select/*+ read_consistency(WEAK)*/ * from dist_t where c1 between 3 and 5;

select * from t1 where c1 in (1, 5);
select * from t1 where (c1, c2) in ((2,2),(3,3));

select/*+ read_consistency(WEAK)*/ * from dist_t where (c1, c2) in ((2,2),(3,3));

select * from t1 where c1 = (null is null);
select * from t1 where c1 = (null is false);
select * from t1 where c1 = ((0>1) is true);
select * from t1 where c1 = (null is unknown);
select * from t1 where c1 = (1 is not null);
select * from t1 where c1 = (null is not null);
select * from t1 where c1 = (null is not false);
select * from t1 where c1 = ((1>0) is not true);
select * from t1 where c1 = (null is not unknown);

select * from t1 where c1 = 1 & 10;
select * from t1 where c1 = 8 | 7;
select * from t1 where c1 = 10 ^ 10;

select * from t1 where exists(select * from t2 where c1 = 7);
select * from t2 where c1 = 2 and c2 = 1 and c3 like '%test%' and c1 between 1 and 5;

#***as
select c1 c1, c2 m from t1 where c1 = 3 or c2 = 1 or c1 < 2;
select s.c1, s.c2+1 from (select c1, c2 from t1) s;
select j1.c1 from t1 as j1, t1 as j2 where j1.c1 = j2.c2 and j1.c1 = 7;
select * from (select * from t1) s;

select/*+ read_consistency(WEAK)*/ j1.c1 from dist_t as j1, dist_t as j2 where j1.c1 = j2.c2 and j1.c1 = 7;

#**group by
select c1, sum(c2), avg(c2), count(c2), max(c2), min(c2) from t1 group by c1;
select c1, sum(c2) from t1 group by c1+3 order by c1+2;
select c1, sum(c2) from t1 group by c1 having c1 > 5;
select c2+1 as c5, sum(c1) from t1 group by c5 order by c5;
select sum(c1)+sum(c2) from t2 group by c2 having sum(c1) > 5 order by 1;
#result sort is different
#select/*+ read_consistency(WEAK)*/ sum(c1)+sum(c2) from dist_t group by c2 having sum(c1) > 5;

#** having
select c1, sum(c2) from t1 group by c1 having sum(c2) > 5;
select * from t1 having c1 = 3;
#remote plan
select/*+ read_consistency(WEAK)*/ * from dist_t having c1 = 2;

#** order by
select * from t1 where c1 = 5 order by c2;
select * from t1 order by 1;
SELECT c1 FROM t1 ORDER BY SUM(c1) + 10;
SELECT c1 FROM t1 ORDER BY SUM(c1 + 10);
select * FROM t1  ORDER BY c1 desc LIMIT 10;
select c1, sum(c2) from t1 group by c1 order by 2 desc, 1 asc;

select/*+ read_consistency(WEAK)*/ c1, sum(c2) from dist_t group by c1 order by 2 desc, 1 asc LIMIT 2;

#** limit
select * from t1 limit 5;
select c1, c2 from t1 limit 7, 2;

#** join
SELECT * FROM t1 INNER JOIN t2 on t1.c1=t2.c1 order by t1.c1;
select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 5 then t2.c2 else t2.c2 - 6 end order by t1.c1;
SELECT * from t1 JOIN t2 on t1.c1 = t2.c1 order by t1.c1;
SELECT * from t1 CROSS JOIN t2 on t1.c1=t2.c1 order by t1.c1;
SELECT * from t1 LEFT JOIN t2 on t1.c1=t2.c1 order by t1.c1;
#sort diff
#SELECT * FROM t1 LEFT JOIN t2 ON t1.c1=t2.c2 WHERE not(0+(t1.c1=3 and t2.c2=4)) order by t1.c1;
SELECT * from t1 RIGHT JOIN t2 on t1.c1=t2.c1 order by t2.c1;

#sort diff
#SELECT/*+ read_consistency(WEAK)*/ * from dist_t JOIN t2 on dist_t.c1 = t2.c1 order by dist_t.c1;

#** union
select * from t1 union select * from t1 union select * from t1;
select * from t1 union all select * from t1;

#** except
#mysql not support
#select c1 from t1 except select c1 from t2 where c1 = 2;

#mysql not suport
#select/*+ read_consistency(WEAK)*/ c1 from t1 except select c1 from dist_t where c1 = 2;

#** intersect
#mysql not support
#select c1 from t1 intersect select c1 from t2 where c1 = 2 or c1 = 3;

#mysql not suport
#select/*+ read_consistency(WEAK)*/ c1 from t1 intersect select c1 from dist_t where c1 = 2 or c1 = 3;

#** for update
select * from t1 for update nowait;
select c1 from t1 where c1 = 10 for update;
#mysql not support
#select c1 from t1 where c1 = 10 for update wait 2;

#mysql not support
#select/*+ read_consistency(WEAK)*/ c1 from dist_t where c1 = 3 for update wait 1;

#** subquery
select * from t1 where t1.c1 > any(select t2.c2 from t2);
select c1, c2, c1 in (select c2 from t1), c1 = all(select c1 from t1) from t1;
select t1.c1 from t1, (select * from t2) as v order by t1.c1;
select * from t1, t2 where t1.c1 > exists(select c2 from t2 where t2.c1 = t1.c1);
#ob not support dist
#select/*+ read_consistency(WEAK)*/ * from t1, t2 where t1.c1 > exists(select c2 from dist_t where dist_t.c1 = t1.c1);

select * from t1 where (0 = 1) or c1 = 1;
select * from t1 where 0 or c1 = 1;
select c1, c2 from t1 group by c1 having (1 = 0 or c1 > 5);
select * from t1 join t2 on (1 = 1 or t1.c1 = t2.c1) order by t1.c1;
select * from t1 where c1 in (select c1 from t2 where (1 = 0 or c1 > 5));

select/*+QUERY_TIMEOUT(111111111)*/c1 from t1 where c1 > 5;
select/*+QUERY_TIMEOUT(100000000)*/c1 from t1 where c1 = 5;

select * from t2 where c3 like '%he%' escape NULL;
select * from t2 where c3 not like '%he%' escape NULL;

#########  DELETE
delete from t1 where c1 = 9;
delete from t1 where c1 > 10;
#delete from t1;

#ob not support
#delete from dist_t where c1 > 10;

#########  UPDATE
update t1 set c2 = 5 where c1 = 5;
update t1 t set t.c2 = 10 where c1 = 10;
update t2 set c2 = c2 + 10, c3 = 'test' where c1 = 5;
update t2 set c2 = c3 like '%h%' where c1 = 3;
update t2 set c2 = 5 where c2 = 1;

#ob not support
#update dist_t set c2 = 10 where c1 = 4 or c1 = 2;

#########  INSERT
truncate table t2;
insert into t2 values(11, 11, 'hello');
insert into t2(c1, c2, c3) values(4, 1, 'test');
insert into t2(c1, c2, c3) values(6, 1, 'test'), (8, 2, 'hello'), (12, 3, 'world');

#values 后面有空格,所以不匹配上面的insert into t2 values(11, 11, 'hello');
insert into t2 values (21, null, null);
insert into t2 values(25, null+null, 1);
insert into t2 values(27, null, 1+null), (29, null, 1);
##expect 0, because 1+null and null + 1 need mache type
insert into t2 values(32, 1, 1+1);

replace into t2 values(33, 1, 'hello'),(34, 1, NULL);

#delete from dist_t where c1 > 10;
#ob not support
#insert into dist_t values(15, 13), (16,14);

#########  REPLACE
replace into t1 values(12, 1);
replace into t1 values(14, 2), (15, 1+1);

#ob not support
#replace into dist_t values(20, 9999), (21, 999);
