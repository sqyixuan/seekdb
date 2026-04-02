## NLJ算子右支正交except算子，扫主表数据
SELECT /*+ leading(t1,T) use_nl(t1, T)*/ t1.*
FROM t1
 JOIN (
    SELECT * FROM t2 where t2.c1 > 100
    MINUS
    SELECT * FROM t2 where t2.c1 > 150
) T
ON t1.c1 = T.c1;

## NLJ算子右支正交except算子，扫local索引, 不回表
SELECT /*+ leading(t1,T) use_nl(t1, T)*/ t1.*
FROM t1
 JOIN (
    SELECT c2 FROM t2 where t2.c2 > 100
    MINUS
    SELECT c2 FROM t2 where t2.c2 > 150
) T
ON t1.c1 = T.c2;

SELECT /*+ leading(t1,T) use_nl(t1, T)*/ t1.*
FROM t1
 JOIN (
    SELECT c2 FROM t2 where t2.c2 > 100
    MINUS
    SELECT c2 FROM t2 where t2.c2 > 150
) T
ON t1.c1 = T.c2;

## NLJ算子右支正交except算子，扫全局索引， 不回表
SELECT /*+ leading(t1,T) use_nl(t1, T)*/ t1.*
FROM t1
 JOIN (
    SELECT c3 FROM t2 where t2.c3 > 100
    MINUS
    SELECT c3 FROM t2 where t2.c3 > 150
) T
ON t1.c1 = T.c3;

## NLJ算子右支正交except算子，扫local索引， 回表
SELECT /*+ leading(t1,T) use_nl(t1, T)*/ t1.*
FROM t1
 JOIN (
    SELECT c2,c3 FROM t2 where t2.c2 > 100
    MINUS
    SELECT c2,c3 FROM t2 where t2.c2 > 150
) T
ON t1.c1 = T.c2;

## NLJ算子右支正交except算子，扫全局索引，回表
SELECT /*+ leading(t1,T) use_nl(t1, T)*/ t1.*
FROM t1
 JOIN (
    SELECT c2,c3 FROM t2 where t2.c3 > 100
    MINUS
    SELECT c2,c3 FROM t2 where t2.c3 > 150
) T
ON t1.c1 = T.c3;


## NLJ算子位于except算子的右支，NLJ算子走batch，可能会被整体短路, tsc算子扫主表
select /*+ use_nl(t1,t2) leading(t1,t2)*/ t1.c2 from t1,t2 where t1.c1 = t2.c2
MINUS
select /*+ use_nl(t3,t4) leading(t3,t4)*/ t3.c2 from t3,t4 where t3.c1 = t4.c2;

## NLJ算子位于except算子的右支，NLJ算子走batch，可能会被整体短路, tsc算子走local索引回表
select /*+ use_nl(t1,t2) leading(t1,t2)*/ t2.c2,t2.c4 from t1,t2 where t1.c1 = t2.c2
MINUS
select /*+ use_nl(t3,t4) leading(t3,t4)*/ t4.c2, t4.c4 from t3,t4 where t3.c1 = t4.c2;

## NLJ算子位于except算子的右支，NLJ算子走batch，可能会被整体短路, tsc算子走全局索引回表
select /*+ use_nl(t1,t2) leading(t1,t2)*/ t2.c3, t2.c4 from t1,t2 where t1.c1 = t2.c3
MINUS
select /*+ use_nl(t3,t4) leading(t3,t4)*/ t4.c3, t4.c4 from t3,t4 where t3.c1 = t4.c3;

## except算子叠加NLJ叠加except算子
SELECT /*+ leading(t1,T) use_nl(t1, T)*/ t1.*
FROM t1
 JOIN (
    SELECT c2,c3 FROM t2 where t2.c3 > 100
    MINUS
    SELECT c2,c3 FROM t2 where t2.c3 > 150
) T
ON t1.c1 = T.c3
MINUS
SELECT /*+ leading(t3,T) use_nl(t3, T)*/ t3.*
FROM t3
 JOIN (
    SELECT c2,c3 FROM t4 where t4.c3 > 100
    MINUS
    SELECT c2,c3 FROM t4 where t4.c3 > 140
) T
ON t3.c1 = T.c3;

##SPF相关短路case

##SPF短路正交扫主表
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c2 from t2 where t2.c1 = t1.c1 and t2.c2 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c1 = t1.c1 and t3.c1 > 8);

##SPF短路正交扫local索引
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c2 from t2 where t2.c2 = t1.c1 and t2.c2 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c2 = t1.c1 and t3.c1 > 8);

##SPF短路正交扫global
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c3 from t2 where t2.c3 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c3 = t1.c1 and t3.c1 > 8);

##SPF短路正交扫local索引回表
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c2 from t2 where t2.c3 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c2 from t3 where t3.c3 = t1.c1 and t3.c1 > 8);

##SPF短路正交扫global索引回表
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c3 from t2 where t2.c2 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c2 = t1.c1 and t3.c1 > 8);

## SPF正交minus,右支扫主表
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c3 from t2 where t2.c2 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c2 = t1.c1 and t3.c1 > 8)
MINUS
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c3 from t2 where t2.c2 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c2 = t1.c1 and t3.c1 > 12);

## SPF正交minus，右支全局索引回表
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c2 from t2 where t2.c3 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c2 from t3 where t3.c3 = t1.c1 and t3.c1 > 8)
MINUS
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c2 from t2 where t2.c3 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c2 from t3 where t3.c3 = t1.c1 and t3.c1 > 12);

## SPF正交minus, 右支local索引回表
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c3 from t2 where t2.c2 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c2 = t1.c1 and t3.c1 > 8)
MINUS
select /*+ no_rewrite */ t1.c1 from t1 where exists (select t2.c3 from t2 where t2.c2 = t1.c1 and t2.c1 > 10 and t2.c1 < 20) and t1.c3 in (select t3.c3 from t3 where t3.c2 = t1.c1 and t3.c1 > 12);