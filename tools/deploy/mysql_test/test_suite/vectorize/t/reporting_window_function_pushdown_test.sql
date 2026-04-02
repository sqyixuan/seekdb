--sleep 3
--explain_protocol 1

select /*+parallel(3) */c1, sum(c2) over (partition by c1) from t1 order by c1;

select /*+parallel(3) */distinct(c1), sum(c2) over (partition by c1) from t1 order by c1;

select * from (select /*+parallel(3) */c1 as cc1, sum(c2) over (partition by c1) as cc2 from t1) v group by v.cc1, v.cc2 order by cc1;

select * from (select /*+parallel(3) */c1 as cc1, count(c2) over (partition by c1) as cc2 from t1) v group by v.cc1, v.cc2 order by cc1;

select * from (select /*+parallel(3) */c1 as cc1, sum(distinct c2) over (partition by c1) as cc2 from t1) v group by v.cc1, v.cc2 order by cc1;

select * from (select /*+parallel(3) */c1 as cc1, count(distinct c2) over (partition by c1) as cc2 from t1) v group by v.cc1, v.cc2 order by cc1;

select * from (select /*+parallel(3) */c1 as cc1, min(c2) over (partition by c1) as cc2 from t1) v group by v.cc1, v.cc2 order by cc1;

select * from (select /*+parallel(3) */c1 as cc1, max(c2) over (partition by c1) as cc2 from t1) v group by v.cc1, v.cc2 order by cc1;

--explain_protocol 0
