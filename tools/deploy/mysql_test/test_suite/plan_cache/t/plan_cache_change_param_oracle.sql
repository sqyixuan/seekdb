
#########  SELECT
#** for cast
select * from type_t where char_c = cast(100 as char(5));
select * from type_t where char_c = cast(100 as char(4));

select '134' from dual;

select * from type_t where date_c = TO_DATE('2015-01-02', 'YYYY-MM-DD');
select * from type_t where time_c = timestamp'1970-01-02 08:00:00.000001';

select 1 from dual where CAST((ROUND(10.2346, 0)) AS CHAR(100)) = '11';
select * from type_t where int_c = 1;
select * from type_t where smallint_c = 2;
select * from type_t where integer_c = 1;
select * from type_t where float_c = 0.10;
select * from type_t where double_c = 0.10;
select * from type_t where decimal_c = 0.10;
select * from type_t where char_c = 'goodie';
select * from type_t where varchar_c = 'goodie';
select * from type_t where binary_c = 'goodie';
select * from type_t where date_c = date'2015-01-02';
select * from type_t where time_c = timestamp'970-01-02 08:00:00.000001';

#** function
select * from type_t where time_c = current_timestamp;
select date_c from type_t where time_c = current_timestamp(1);
select date_c, time_c from type_t where time_c = current_timestamp(1);

select * from type_t where varchar_c = concat('hello', 'worldhello');
select * from type_t where varchar_c = concat('hello', NULL);
select * from type_t where varchar_c = substr('ahello', 2);
select * from type_t where varchar_c = substr('abhello', 2, 5);
#for trim
select * from type_t where varchar_c = trim(' hello  ');
select * from type_t where varchar_c = trim(leading 'x' from 'xxxhelloworldxx');
select * from type_t where varchar_c = trim(leading from '  hello  ');
select * from type_t where varchar_c = trim(trailing 'x' from 'xxxhelloworldxx');
select * from type_t where varchar_c = trim(trailing from '  hello  ');
select * from type_t where varchar_c = trim(both 'x' from 'xxxhelloworldxx');
select * from type_t where varchar_c = trim(both from '  hello  ');

select * from type_t where int_c = length('helloworld');
select * from type_t where int_c = length(-1.23);
select * from type_t where varchar_c = upper('hello');
select * from type_t where varchar_c = lower('HELLO');
select * from type_t where 'abcd' like '%bc';
select * from type_t where 'abcd' not like '%bc';
select * from type_t where 'abc%' like 'abc%' escape 'c';

select * from type_t where varchar_c = lpad('1', 3, '1');
select * from type_t where varchar_c = rpad('1', 3, '1');

select * from type_t where int_c = instr('foobarbr', 'bar');
select * from type_t where varchar_c = replace('xxxworld', 'xxx', 'hel');

select * from type_t where int_c = round(1.11);
select * from type_t where double_c = round(111.1e-2,1);
select * from type_t where int_c = ceil(0.9);
select * from type_t where int_c = floor(1.11);
select * from type_t where int_c = abs(-111);
select * from type_t where int_c = sign(-222);

select * from type_t where int_c = greatest(2, '1.23', 0);
select * from type_t where int_c = greatest(2.0, 1, 0);
select * from type_t where int_c = least(2, '1', 0);
select * from type_t where int_c = least(1, 1.11, 0);

select c1, case c1 when 1 then 'is 1' else 'not 1' end from t1 where c1 = 1;
# k3:r6538287
select c1 from t1 where c1 = 2;

#**operator
select * from t1 where c1 = MOD((2 + 2 -2)*2/2, 5);
select * from t1 where c1 = case when 2 > 3 then 1 else 0 end and (3 > 4) or (4 > 5);
select * from t1 where c1 != 3;


select/*+ read_consistency(WEAK)*/ * from dist_t where c1 = case when ((2 < 4) and (3 > 4)) then 1 else 0 end or (5 > 5);
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 = case when (c1 >= 2 and c2 <= 2) then 1 else 0 end or c1 = 1;
select/*+ read_consistency(WEAK)*/ * from dist_t where c1 != 1;

select * from t1 where c1 not between 1 and 2;
select * from t1 where c1 between 3 and 0;
select * from t1 where c1 between null and 3;

select/*+ read_consistency(WEAK)*/ * from dist_t where c1 between 0 and 3;

select * from t1 where c1 in (1, 2);
select * from t1 where (c1, c2) in ((2,1),(5,3));

select/*+ read_consistency(WEAK)*/ * from dist_t where (c1, c2) in ((2,1),(5,3));

select * from t1 where c1 = case when (null is null) then 1 else 0 end;
select * from t1 where c1 = case when (0 is not null) then 1 else 0 end;
select * from t1 where c1 = case when (null is not null) then 1 else 0 end;

select * from t1 where c1 = bitand(1, 5);

select * from t1 where exists(select * from t2 where c1 = 1);
select * from t2 where c1 = 1 and c2 = 1 and c3 like '%test%' and c1 between 1 and 10;

#***as
select c1 c1, c2 as m from t1 where c1 = 2 or c2 = 2 or c1 < 5;
select s.c1, s.c2+1 from (select c1, c2 from t1) s;
select j1.c1 from t1 j1, t1 j2 where j1.c1 = j2.c2 and j1.c1 = 0;
select * from (select * from t1) s;

select/*+ read_consistency(WEAK)*/ j1.c1 from dist_t j1, dist_t j2 where j1.c1 = j2.c2 and j1.c1 = 2;

#**group by
select c1, sum(c2), avg(c2), count(c2), max(c2), min(c2) from t1 group by c1;
select c1, sum(c2) from t1 group by c1 order by c1;
select c1, sum(c2) from t1 group by c1 having c1 > 1;
select sum(c1)+c2 from t2 group by c2 having sum(c1) > 6 order by 1;

#** having
select c1, sum(c2) from t1 group by c1 having sum(c2) > 4;

#** order by
select * from t1 where c1 = 1 order by c2;
select * from t1 order by 2;
select c1, sum(c2) from t1 group by c1 order by 1 desc, 2 asc;

#** join
SELECT * FROM t1 INNER JOIN t2 on t1.c1=t2.c1 order by t1.c1;
select * from t1 inner join t2 on t1.c1 = t2.c1 where t1.c1 > case when t1.c1 = t2.c2 - 2 then t2.c2 else t2.c2 - 1 end order by t1.c1;
SELECT * from t1 JOIN t2 on t1.c1 = t2.c1 order by t1.c1;
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
select t1.c1 from t1, (select * from t2) v order by t1.c1;
select * from t1, t2 where exists(select c2 from t2 where t2.c1 = t1.c1);

select * from t1 where (1 = 1) or c1 = 2;
select c1, sum(c2) from t1 group by c1 having (1 = 1 or c1 > 4);
select * from t1 join t2 on (1 = 1 or t1.c1 = t2.c1) order by t1.c1;
select * from t1 where c1 in (select c1 from t2 where (1 = 1 or c1 > 5));

select/*+QUERY_TIMEOUT(100000000)*/c1 from t1 where c1 > 5;
select/*+QUERY_TIMEOUT(100000000)*/c1 from t1 where c1 = 4;

#########  DELETE
delete from t1 where c1 = 9;
delete from t1 where c1 > 9;

#########  UPDATE
update t1 set c2 = 1 where c1 = 2;
update t1 t set t.c2 = 1 where c1 = 2;
update t2 set c2 = c2 + 100, c3 = 'testtest' where c1 = 1;
update t2 set c2 = 10 where c2 = 2;

#ob not support
#update dist_t set c2 = 10 where c1 = 1 or c1 = 2;

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

### acs case
select * from acs_test where c2 = 1;
