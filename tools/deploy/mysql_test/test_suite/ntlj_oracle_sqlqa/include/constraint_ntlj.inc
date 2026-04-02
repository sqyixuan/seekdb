--echo	#约束中使用NTLJ算子
--echo	#无条件链接
SELECT count(*) FROM t1,t2;
#explain basic  SELECT count(*) FROM t1,t2;

SELECT count(*) FROM t1 NATURAL JOIN t2;
#explain basic  SELECT count(*) FROM t1 NATURAL JOIN t2;

SELECT count(*) FROM t1 CROSS JOIN t2;
#explain basic  SELECT count(*) FROM t1 CROSS JOIN t2;

SELECT count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2),(SELECT * FROM t1 CROSS JOIN t2);
#explain basic  SELECT count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2),(SELECT * FROM t1 CROSS JOIN t2);

SELECT count(*) FROM (SELECT * FROM t1,t2),(SELECT * FROM t1 NATURAL JOIN t2);
#explain basic  SELECT count(*) FROM (SELECT * FROM t1,t2),(SELECT * FROM t1 NATURAL JOIN t2);

SELECT count(*) FROM (SELECT * FROM t1,t2) NATURAL JOIN (SELECT * FROM t1,t2);
#explain basic  SELECT count(*) FROM (SELECT * FROM t1,t2) NATURAL JOIN (SELECT * FROM t1,t2);

SELECT count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2);
#explain basic  SELECT count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2);

SELECT count(*) FROM (SELECT * FROM t1,t2) CROSS JOIN (SELECT * FROM t1 NATURAL JOIN t2);
#explain basic  SELECT count(*) FROM (SELECT * FROM t1,t2) CROSS JOIN (SELECT * FROM t1 NATURAL JOIN t2);

SELECT count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) CROSS JOIN (SELECT * FROM t1 CROSS JOIN t2);
#explain basic  SELECT count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) CROSS JOIN (SELECT * FROM t1 CROSS JOIN t2);

SELECT count(*) FROM (SELECT * FROM t1 CROSS JOIN t2) CROSS JOIN (SELECT * FROM t1,t2);
#explain basic  SELECT count(*) FROM (SELECT * FROM t1 CROSS JOIN t2) CROSS JOIN (SELECT * FROM t1,t2);

SELECT /*+parallel(4)*/ count(*) FROM t1,t2;
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM t1,t2;



SELECT /*+parallel(4)*/ count(*) FROM t1 NATURAL JOIN t2;
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM t1 NATURAL JOIN t2;

SELECT /*+parallel(4)*/ count(*) FROM t1 CROSS JOIN t2;
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM t1 CROSS JOIN t2;

SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2),(SELECT * FROM t1 CROSS JOIN t2);
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2),(SELECT * FROM t1 CROSS JOIN t2);

SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1,t2),(SELECT * FROM t1 NATURAL JOIN t2);
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1,t2),(SELECT * FROM t1 NATURAL JOIN t2);

SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1,t2) NATURAL JOIN (SELECT * FROM t1,t2);
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1,t2) NATURAL JOIN (SELECT * FROM t1,t2);

SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2);
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2);

SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1,t2) CROSS JOIN (SELECT * FROM t1 NATURAL JOIN t2);
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1,t2) CROSS JOIN (SELECT * FROM t1 NATURAL JOIN t2);

SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) CROSS JOIN (SELECT * FROM t1 CROSS JOIN t2);
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 NATURAL JOIN t2) CROSS JOIN (SELECT * FROM t1 CROSS JOIN t2);

SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 CROSS JOIN t2) CROSS JOIN (SELECT * FROM t1,t2);
#explain basic  SELECT /*+parallel(4)*/ count(*) FROM (SELECT * FROM t1 CROSS JOIN t2) CROSS JOIN (SELECT * FROM t1,t2);



--echo	#内连接
SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a2) FROM t1,t2) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2),READ_CONSISTENCY (weak)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a2) FROM t1,t2) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t2.a2 > (SELECT max(c1) FROM (SELECT * FROM t1 CROSS JOIN t2) NATURAL JOIN (SELECT * FROM t1,t2)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t2.a2 > (SELECT max(c1) FROM (SELECT * FROM t1 CROSS JOIN t2) NATURAL JOIN (SELECT * FROM t1,t2)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c2 != (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c2 != (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <> (select /*+use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <> (select /*+use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a2) FROM t1,t2) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2),READ_CONSISTENCY (weak)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a2) FROM t1,t2) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t2.a2 > (SELECT max(c1) FROM (SELECT * FROM t1 CROSS JOIN t2) NATURAL JOIN (SELECT * FROM t1,t2)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t2.a2 > (SELECT max(c1) FROM (SELECT * FROM t1 CROSS JOIN t2) NATURAL JOIN (SELECT * FROM t1,t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c2 != (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c2 != (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <> (select /*+parallel(6) use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <> (select /*+parallel(6) use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;


select /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;

select /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

select /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;

SELECT /*+use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 =(select max(a1) from t2)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 =(select max(a1) from t2)) order by t1.c1;

SELECT /*+use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 != (select /*+use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;
#explain basic  SELECT /*+use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 != (select /*+use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 =(select max(a1) from t2)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 =(select max(a1) from t2)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 != (select /*+use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 != (select /*+use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;


select /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 > (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 > (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a1 <> (select /*+parallel(6) use_nl(t1,t2)*/ c1 from t1  where c1 =(select max(a1) from t2)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a1 <> (select /*+parallel(6) use_nl(t1,t2)*/ c1 from t1  where c1 =(select max(a1) from t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 != (select /*+parallel(6) use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t2,t1)*/ * FROM t2 LEFT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 != (select /*+parallel(6) use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 < (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 < (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+parallel(6) use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+parallel(6) use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+parallel(6) use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 != (select /*+parallel(6) use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 != (select /*+parallel(6) use_nl(t1,t2)*/min(a2) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL) order by t1.c1;


SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2))) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2))) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1 ;

SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2))) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2))) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1;

SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;
#explain basic  SELECT /*+use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;


SELECT /*+parallel(6) use_nl(t1,t2) pq_distribute(t1 none all)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)))  order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2)))  order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t2 RIGHT JOIN t1 ON t2.a1 = t1.c1 WHERE t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c1 = t2.a1 WHERE t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2) pq_distribute(t1 none all)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2))) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t1.c2 > (SELECT /*+use_nl(t1,t2)*/ max(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 <(SELECT max(a1) FROM t1,t2))) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c1 < (select /*+use_nl(t1,t2)*/ max(c1) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1 WHERE t2.a2 < (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a1 = (SELECT /*+use_nl(t1,t2)*/ min(c2) FROM t1,t2 WHERE t1.c1 = t2.a1 AND t1.c1 = (SELECT /*+use_nl(t1,t2)*/ max(c1) FROM t1 right JOIN t2 ON t1.c1 = t2.a1)) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t2.a2 <> (SELECT /*+use_nl(t1,t2)*/ count(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a2 = (SELECT max(c1) FROM (SELECT * FROM t1 NATURAL JOIN t2)NATURAL JOIN (SELECT * FROM t1 CROSS JOIN t2))) order by t1.c1;

SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;
#explain basic  SELECT /*+parallel(6) use_nl(t1,t2)*/ * FROM t1,t2 WHERE t1.c1(+) = t2.a1 AND t1.c2 != (SELECT /*+use_nl(t1,t2)*/ min(c1) FROM t1,t2 WHERE t1.c1 = t2.a1(+) AND t2.a1 <> (select /*+use_nl(t1,t2)*/ c1 from t1  where c1 <=(select min(a1) from t2))) order by t1.c1;

--echo	#SEMI JOIN
select /*+use_nl(t1,t2)*/* from t1  where c1 in(select a1 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 in(select a1 from t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c2 IN (SELECT c2 FROM t1 NATURAL JOIN t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c2 IN (SELECT c2 FROM t1 NATURAL JOIN t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1 CROSS JOIN t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1 CROSS JOIN t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t2  where a1 IN (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1) order by t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/* from t2  where a1 IN (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1) order by t2.a1;

select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t2  where a2 IN (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+)) order by t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/* from t2  where a2 IN (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+)) order by t2.a1;

select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1) order by t1.c1;




select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 in(select a1 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 in(select a1 from t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c2 IN (SELECT c2 FROM t1 NATURAL JOIN t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c2 IN (SELECT c2 FROM t1 NATURAL JOIN t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1 CROSS JOIN t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1 CROSS JOIN t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a1 IN (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1) order by t2.a1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a1 IN (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1) order by t2.a1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1)order by c1 ;

select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a2 IN (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+)) order by t2.a1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a2 IN (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+)) order by t2.a1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by c1 ;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1)order by c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 IN (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1)order by c1;




select /*+use_nl(t1,t2)*/* from t1  where c1 = any(select a1 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 = any(select a1 from t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c2 = any (SELECT c2 FROM t1 NATURAL JOIN t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c2 = any (SELECT c2 FROM t1 NATURAL JOIN t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1 CROSS JOIN t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1 CROSS JOIN t2) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t2  where a1 = any (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1) order by t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/* from t2  where a1 = any (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1) order by t2.a1;

select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t2  where a2 = any (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+)) order by t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/* from t2  where a2 = any (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+)) order by t2.a1;

select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1) order by t1.c1;





select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any(select a1 from t2) order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any(select a1 from t2)order by c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2)order by c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c2 = any (SELECT c2 FROM t1 NATURAL JOIN t2)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c2 = any (SELECT c2 FROM t1 NATURAL JOIN t2)order by c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1 CROSS JOIN t2)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1 CROSS JOIN t2)order by c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a1 = any (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1)order by a1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a1 = any (SELECT a1 FROM t1,t2 WHERE t1.c1 = t2.a1)order by a1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 LEFT JOIN t2 ON t1.c1 = t2.a1)order by c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a2 = any (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+))order by a1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t2  where a2 = any (SELECT c1 FROM t1,t2 WHERE t1.c1 = t2.a1(+))order by a1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT a2 FROM t1 right JOIN t2 ON t1.c1 = t2.a1)order by c1;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1)order by c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 = any (SELECT c1 FROM t1,t2 WHERE t1.c1(+) = t2.a1)order by c1;



--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/distinct * from t1 join t2 on t1.c1=t2.a1 ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/distinct * from t1 join t2 on t1.c1=t2.a1 ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1  where c1 >(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 >(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1  where c1 <(select max(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 <(select max(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1  where c1 >=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 >=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1  where c1 <=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 <=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1  where c1 !=(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 !=(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1  where c1 <> (select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 <>(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;

select /*+parallel(6)  use_nl(t1,t2)*/distinct * from t1 join t2 on t1.c1=t2.a1 ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/distinct * from t1 join t2 on t1.c1=t2.a1 ORDER BY t1.c1,t1.c2;
select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 >(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 >(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 <(select max(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 <(select max(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 >=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 >=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 <=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 <=(select min(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 !=(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 !=(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 <> (select avg(a1) from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6)  use_nl(t1,t2)*/* from t1  where c1 <>(select avg(a1) from t2) ORDER BY t1.c1,t1.c2;


--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/* from t1  where c1 not in(select a1 from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 not in(select a1 from t2) ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1  where c1 != all(select a1 from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  where c1 != all(select a1 from t2) ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is NULL ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is NULL ORDER BY t1.c1,t1.c2;
select /*+use_nl(t1,t2)*/* from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL ORDER BY t1.c1,t1.c2;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL ORDER BY t1.c1,t1.c2;

select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 not in(select a1 from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 not in(select a1 from t2) ORDER BY t1.c1,t1.c2;
select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 != all(select a1 from t2) ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1  where c1 != all(select a1 from t2) ORDER BY t1.c1,t1.c2;
select /*+parallel(6) use_nl(t1,t2)*/* from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is NULL ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is NULL ORDER BY t1.c1,t1.c2;
select /*+parallel(6) use_nl(t1,t2) pq_distribute(t1 none all)*/* from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL ORDER BY t1.c1,t1.c2;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/* from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is NULL ORDER BY t1.c1,t1.c2;




