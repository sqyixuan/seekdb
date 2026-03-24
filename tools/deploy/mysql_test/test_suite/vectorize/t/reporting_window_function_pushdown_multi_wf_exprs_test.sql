--sleep 3
--explain_protocol 1

select /*+parallel(3) */c1, c2, c3, c4, max(c1) over (partition by c1), min(c2) over (partition by c1), sum(c3) over (partition by c1), count(c4) over (partition by c1) from t1 order by c1, c2, c3, c4;

select /*+parallel(3) */c1, c2, c3, c4, max(c1) over (partition by c1, c2), min(c2) over (partition by c1, c2), sum(c3) over (partition by c1, c2), count(c4) over (partition by c1, c2) from t1 order by c1, c2, c3, c4;

select /*+parallel(3) */c1, c2, c3, c4, max(c1) over (partition by c1, c2, c3), min(c2) over (partition by c1, c2, c3), sum(c3) over (partition by c1, c2, c3), count(c4) over (partition by c1, c2, c3) from t1 order by c1, c2, c3, c4;

select /*+parallel(3) */c1, c2, c3, c4, max(c1) over (partition by c1, c2, c3, c4), min(c2) over (partition by c1, c2, c3, c4), sum(c3) over (partition by c1, c2, c3, c4), count(c4) over (partition by c1, c2, c3, c4) from t1 order by c1, c2, c3, c4;

select /*+parallel(3) */c1, c2, c3, c4, count(c4) over (partition by c1, c2, c3, c4), sum(c3) over (partition by c1, c2, c3), min(c2) over (partition by c1, c2), max(c1) over (partition by c1) from t1 order by c1, c2, c3, c4;

select /*+parallel(3) */c1, c2, c3, c4, count(c4) over (partition by c1, c2, c3, c4), sum(c3) over (partition by c1, c2, c3) from t1 order by c1, c2, c3, c4;

--explain_protocol 0

