--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI NA

SELECT * FROM t1 WHERE DBMS_LOB.substr(c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY  t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 WHERE DBMS_LOB.substr(c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI SNA

SELECT * FROM t1 WHERE DBMS_LOB.substr(c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 WHERE DBMS_LOB.substr(c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #INNER JOIN
--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI NA

SELECT * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo ##IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI SNA

SELECT * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #RIGHT JOIN
--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI NA

SELECT * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 AND NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI SNA

SELECT * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 AND NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #LEFT JOIN
--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI NA

SELECT * FROM t1 LEFT JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 LEFT JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 LEFT JOIN t2 ON t1.c0=t2.c0 WHERE NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo ##IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI SNA

SELECT * FROM t1 LEFT JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 LEFT JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 LEFT JOIN t2 ON t1.c0=t2.c0 WHERE NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #FULL JOIN
--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI NA

SELECT * FROM t1 FULL JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 FULL JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 FULL JOIN t2 ON t1.c0=t2.c0 WHERE NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI SNA

SELECT * FROM t1 FULL JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 FULL JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT * FROM t1 FULL JOIN t2 ON t1.c0=t2.c0 WHERE NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #HINT
--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI NA

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI SNA

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 JOIN t2 ON t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 RIGHT JOIN t2 ON t1.c0=t2.c0 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+USE_HASH(t1,t2)*/ * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #PARALLEL
--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI NA

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_MJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_NLJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_NLJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_MJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_HASH(t1,t2)*/ * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1,t2 WHERE t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1,t2 WHERE t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1,t2 WHERE t1.c0=t2.c0 AND NOT EXISTS (SELECT c1 FROM t2) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

--echo #IF "_optimizer_null_aware_antijoin"=TRUE,EXPLAIN:HASH JOIN ANTI SNA

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_MJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_NLJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_NLJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_HASH(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_MJ(t1,t2)*/ * FROM t1 WHERE DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(5) USE_HASH(t1,t2)*/ * FROM t1 WHERE NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1,t2 WHERE t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) NOT IN (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1,t2 WHERE t1.c0=t2.c0 AND DBMS_LOB.substr(t1.c1) <>ALL (SELECT DBMS_LOB.substr(c1) FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);

SELECT /*+parallel(2) USE_HASH(t1,t2)*/ * FROM t1,t2 WHERE t1.c0=t2.c0 AND NOT EXISTS (SELECT c1 FROM t2 WHERE  DBMS_LOB.substr(t2.c1) IS NOT NULL) ORDER BY t1.c0,DBMS_LOB.substr(t1.c1);


