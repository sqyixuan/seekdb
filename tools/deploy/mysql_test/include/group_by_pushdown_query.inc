select count(*), count(c1), count(c2), count(c3) from t1;
select count(*), count(c1), count(c2), count(c3) from t2;
select count(*), count(c1), count(c2), count(c3) from t3;
select count(*), count(c1), count(c2), count(c3) from pt1;
select count(*), count(c1), count(c2) from t_padding;
select count(*), count(c1), count(c2) from st1;
select count(*), count(c1), count(c2) from st2;

select count(c1), min(c1), max(c1), sum(c1), count(c2), min(c2), max(c2), sum(c2), count(c3), min(c3), max(c3) from t1;
select count(c1), min(c1), max(c1), sum(c1), count(c2), min(c2), max(c2), sum(c2), count(c3), min(c3), max(c3) from t2;
select count(c1), min(c1), max(c1), sum(c1), count(c2), min(c2), max(c2), sum(c2), count(c3), min(c3), max(c3) from t3;
select count(c1), min(c1), max(c1), sum(c1), count(c2), min(c2), max(c2), sum(c2), count(c3), min(c3), max(c3) from pt1;
select count(c1), min(c1), max(c1), count(c2), min(c2), max(c2) from t_padding;
select count(c1), min(c1), max(c1), sum(c1), count(c2), min(c2), max(c2), sum(c2) from st1;
select count(c1), min(c1), max(c1), sum(c1), count(c2), min(c2), max(c2), sum(c2) from st2;

select count(distinct c1) from t1;
select count(distinct c1) from t2;
select count(distinct c1) from pt1;
select c1 from t1 group by c1 order by c1;
select c2 from t1 group by c2 order by c2;
select c3 from t1 group by c3 order by c3;
select c1 from t2 group by c1 order by c1;
select c2 from t2 group by c2 order by c2;
select c3 from t2 group by c3 order by c3;
select c1 from pt1 group by c1 order by c1;
select c2 from pt1 group by c2 order by c2;
select c3 from pt1 group by c3 order by c3;

select count(*) from t1 group by c1 order by c1;
select c1,count(*) from t1 group by c1 order by c1;
select c1,count(c2) from t1 group by c1 order by c1;
select c1,count(*),count(pk),count(c1),count(c2),count(c3) from t1 group by c1 order by c1;

select c2,count(*) from t1 group by c2 order by c2;
select c2,count(c3) from t1 group by c2 order by c2;
select c2,count(*),count(pk),count(c1),count(c2),count(c3) from t1 group by c2 order by c2;

select c3,count(*) from t1 group by c3 order by c3;
select c3,count(c1) from t1 group by c3 order by c3;
select c3,count(*),count(pk),count(c1),count(c2),count(c3) from t1 group by c3 order by c3;

select c1,min(c1),max(c1),sum(c1) from t1 group by c1 order by c1;
select c1,min(c2),max(c2),sum(c2) from t1 group by c1 order by c1;
select c1,min(c3),max(c3) from t1 group by c1 order by c1;

select c2,min(c1),max(c1),sum(c1) from t1 group by c2 order by c2;
select c2,min(c2),max(c2),sum(c2) from t1 group by c2 order by c2;
select c2,min(c3),max(c3) from t1 group by c2 order by c2;

select c3,min(c1),max(c1),sum(c1) from t1 group by c3 order by c3;
select c3,min(c2),max(c2),sum(c2) from t1 group by c3 order by c3;
select c3,min(c3),max(c3) from t1 group by c3 order by c3;

select c1,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from t1 group by c1 order by c1;
select c2,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from t1 group by c2 order by c2;
select c3,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from t1 group by c3 order by c3;

select count(*) from t2 group by c1 order by c1;
select c1,count(*) from t2 group by c1 order by c1;
select c1,count(c2) from t2 group by c1 order by c1;
select c1,count(*),count(pk),count(c1),count(c2),count(c3) from t2 group by c1 order by c1;

select c2,count(*) from t2 group by c2 order by c2;
select c2,count(c3) from t2 group by c2 order by c2;
select c2,count(*),count(pk),count(c1),count(c2),count(c3) from t2 group by c2 order by c2;

select c3,count(*) from t2 group by c3 order by c3;
select c3,count(c1) from t2 group by c3 order by c3;
select c3,count(*),count(pk),count(c1),count(c2),count(c3) from t2 group by c3 order by c3;

select c1,min(c1),max(c1),sum(c1) from t2 group by c1 order by c1;
select c1,min(c2),max(c2),sum(c2) from t2 group by c1 order by c1;
select c1,min(c3),max(c3) from t2 group by c1 order by c1;

select c2,min(c1),max(c1),sum(c1) from t2 group by c2 order by c2;
select c2,min(c2),max(c2),sum(c2) from t2 group by c2 order by c2;
select c2,min(c3),max(c3) from t2 group by c2 order by c2;

select c3,min(c1),max(c1),sum(c1) from t2 group by c3 order by c3;
select c3,min(c2),max(c2),sum(c2) from t2 group by c3 order by c3;
select c3,min(c3),max(c3) from t2 group by c3 order by c3;

select c2,count(*) from t2 where pk > 1000 group by c2 order by c2;
select c2,count(*) from t2 where pk > 4000 group by c2 order by c2;
select c2,count(*) from t2 where pk < 9000 group by c2 order by c2;
select c2,count(*) from t2 where pk > 6000 group by c2 order by c2;

select c1,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from t2 group by c1 order by c1;
select c2,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from t2 group by c2 order by c2;
select c3,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from t2 group by c3 order by c3;

select count(*) from pt1 group by c1 order by c1;
select c1,count(*) from pt1 group by c1 order by c1;
select c1,count(c2) from pt1 group by c1 order by c1;
select c1,count(*),count(pk),count(c1),count(c2),count(c3) from pt1 group by c1 order by c1;

select c2,count(*) from pt1 group by c2 order by c2;
select c2,count(c3) from pt1 group by c2 order by c2;
select c2,count(*),count(pk),count(c1),count(c2),count(c3) from pt1 group by c2 order by c2;

select c3,count(*) from pt1 group by c3 order by c3;
select c3,count(c1) from pt1 group by c3 order by c3;
select c3,count(*),count(pk),count(c1),count(c2),count(c3) from pt1 group by c3 order by c3;

select c1,min(c1),max(c1),sum(c1) from pt1 group by c1 order by c1;
select c1,min(c2),max(c2),sum(c2) from pt1 group by c1 order by c1;
select c1,min(c3),max(c3) from pt1 group by c1 order by c1;

select c2,min(c1),max(c1),sum(c1) from pt1 group by c2 order by c2;
select c2,min(c2),max(c2),sum(c2) from pt1 group by c2 order by c2;
select c2,min(c3),max(c3) from pt1 group by c2 order by c2;

select c3,min(c1),max(c1),sum(c1) from pt1 group by c3 order by c3;
select c3,min(c2),max(c2),sum(c2) from pt1 group by c3 order by c3;
select c3,min(c3),max(c3) from pt1 group by c3 order by c3;

select c1,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from pt1 group by c1 order by c1;
select c2,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from pt1 group by c2 order by c2;
select c3,count(*),count(pk),count(c1),count(c2),count(c3),min(pk),min(c1),min(c2),min(c3),max(pk),max(c1),max(c2),max(c3),sum(pk),sum(c1),sum(c2) from pt1 group by c3 order by c3;
