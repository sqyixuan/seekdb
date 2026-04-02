--echo	#无条件连接
select count(*) from t1,t2;
#explain basic select count(*) from t1,t2;
select /*+parallel(4)*/ count(*) from t1,t2;
#explain basic select /*+parallel(4)*/ count(*) from t1,t2;

select count(*) from t1 NATURAL JOIN t2;
#explain basic select count(*) from t1 NATURAL JOIN t2;
select /*+parallel(4)*/ count(*) from t1 NATURAL JOIN t2;
#explain basic select /*+parallel(4)*/ count(*) from t1 NATURAL JOIN t2;

select count(*) from t1,t2 where t1.c2 > t2.a2 order by t1.c2;
#explain basic select count(*) from t1,t2 where t1.c2 > t2.a2 order by t1.c2;
select /*+parallel(4)*/ count(*) from t1,t2 where t1.c2 > t2.a2 order by t1.c2;
#explain basic select /*+parallel(4)*/ count(*) from t1,t2 where t1.c2 > t2.a2 order by t1.c2;

select  max(t2.a2) from t1,t2 where t1.c2 > t2.a2;
#explain basic select  max(t2.a2) from t1,t2 where t1.c2 > t2.a2;
select /*+parallel(4)*/ max(t2.a2) from t1,t2 where t1.c2 > t2.a2;
#explain basic select /*+parallel(4)*/ max(t2.a2) from t1,t2 where t1.c2 > t2.a2;

select distinct* from t1,t2 where t1.c2> t2.a2 order by t1.c1 desc;
#explain basic select distinct* from t1,t2 where t1.c2> t2.a2 order by t1.c1 desc;

select /*+parallel(4) use_nl(t2,t1) pq_distribute(t1 none all)*/ distinct* from t1,t2 where t1.c2> t2.a2 order by t1.c1 desc, t2.a1 desc;
#explain basic select /*+parallel(4) use_nl(t2,t1) pq_distribute(t1 none all)*/ distinct* from t1,t2 where t1.c2> t2.a2 order by t1.c1 desc;

select count(*) from (select * from t1 cross join t2)a,(select * from t1,t2)b;
#explain basic select count(*) from (select * from t1 cross join t2)a,(select * from t1,t2)b;
select /*+parallel(4)*/ count(*) from (select /*+parallel(4)*/ * from t1 cross join t2)a,(select /*+parallel(4)*/ * from t1,t2)b;
#explain basic select /*+parallel(4)*/ count(*) from (select /*+parallel(4)*/ * from t1 cross join t2)a,(select /*+parallel(4)*/ * from t1,t2)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;
#explain basic select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;

select count(*) from t1 cross join t2;
#explain basic select count(*) from t1 cross join t2;
select /*+parallel(4)*/ count(*) from t1 cross join t2;
#explain basic select /*+parallel(4)*/ count(*) from t1 cross join t2;

select count(*) from t1 cross join t2 where t1.c2>50;
#explain basic select count(*) from t1 cross join t2 where t1.c2>50;
select /*+parallel(4)*/ count(t1.c2) from t1 cross join t2 where t1.c2>50;
#explain basic select /*+parallel(4)*/ count(t1.c2) from t1 cross join t2 where t1.c2>50;

select count(*) from t1 cross join t2 where t1.c2>50 and t1.c2<t2.a2 order by t1.c2 desc;
#explain basic select count(*) from t1 cross join t2 where t1.c2>50 and t1.c2<t2.a2 order by t1.c2 desc;
select /*+parallel(4)*/ count(*) from t1 cross join t2 where t1.c2>50 and t1.c2<t2.a2 order by t1.c2 desc;
#explain basic select /*+parallel(4)*/ count(*) from t1 cross join t2 where t1.c2>50 and t1.c2<t2.a2 order by t1.c2 desc;

select count(*) from (select * from t1 cross join t2)a cross JOIN (select * from t1,t2)b;
#explain basic select count(*) from (select * from t1 cross join t2)a cross JOIN (select * from t1,t2)b;
select /*+parallel(4)*/ count(*) from (select /*+parallel(4)*/ *  from t1 cross join t2)a cross JOIN (select /*+parallel(4)*/ * from t1,t2)b;
#explain basic select /*+parallel(4)*/ count(*) from (select /*+parallel(4)*/ *  from t1 cross join t2)a cross JOIN (select /*+parallel(4)*/ * from t1,t2)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a cross JOIN (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a cross JOIN (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;
select /*+parallel(4)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a cross JOIN (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;
#explain basic select /*+parallel(4)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a cross JOIN (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b;

--echo	#内连接
select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;
#explain basic select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;
#explain basic select /*+parallel(4) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;

select /*+use_nl(t1,t2)*/ count(*) from t1,t2 where t1.c2 = t2.a2;
#explain basic select /*+use_nl(t1,t2)*/ count(*) from t1,t2 where t1.c2 = t2.a2;
select /*+parallel(4)* use_nl(t1,t2)*/ count(*) from t1,t2 where t1.c2 = t2.a2;
#explain basic select /*+parallel(4)* use_nl(t1,t2)*/ count(*) from t1,t2 where t1.c2 = t2.a2;

select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t2.a2 desc;
#explain basic select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t2.a2 desc;
select /*+parallel(4)* use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t2.a2 desc;
#explain basic select /*+parallel(4)* use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t2.a2 desc;

select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 and t1.c2>30 order by t2.a2 desc;
#explain basic select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 and t1.c2>30 order by t2.a2 desc;
select /*+parallel(4)*  use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 and t1.c2>30 order by t2.a2 desc;
#explain basic select /*+parallel(4)*  use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 and t1.c2>30 order by t2.a2 desc;

select count(*) from (select * from t1 cross join t2)a,(select * from t1,t2)b where a.c2 = b.a2;
#explain basic select count(*) from (select * from t1 cross join t2)a,(select * from t1,t2)b where a.c2 = b.a2;
select count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a,(select /*parallel(6)*/ * from t1,t2)b where a.c2 = b.a2;
#explain basic select count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a,(select /*parallel(6)*/ * from t1,t2)b where a.c2 = b.a2;

select count(*) from (select * from t1 cross join t2)a,(select * from t1,t2)b where a.c2 = b.a2;
#explain basic select count(*) from (select * from t1 cross join t2)a,(select * from t1,t2)b where a.c2 = b.a2;
select count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a,(select /*parallel(6)*/ * from t1,t2)b where a.c2 = b.a2;
#explain basic select count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a,(select /*parallel(6)*/ * from t1,t2)b where a.c2 = b.a2;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;
#explain basic select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a,(select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a,(select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;
#explain basic select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b where a.c2 = b.a2;

select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;

select /*+use_nl(t1,t2)*/ avg(t2.a2) from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic select /*+use_nl(t1,t2)*/ avg(t2.a2) from t1 inner join t2 on t1.c2 = t2.a2 ;
select /*+parallel(6) use_nl(t1,t2)*/ avg(t2.a2) from t1 inner join t2 on t1.c2 = t2.a2 ;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ avg(t2.a2) from t1 inner join t2 on t1.c2 = t2.a2;

select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c2 desc;

select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 where t2.a2<50 order by t1.c2 desc;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 where t2.a2<50 order by t1.c2 desc;

select count(*) from (select * from t1 cross join t2)a inner join (select * from t1,t2)b on  a.c2 = b.a2;
#explain basic select count(*) from (select * from t1 cross join t2)a inner join (select * from t1,t2)b on  a.c2 = b.a2;
select count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a inner join  (select /*parallel(6)*/ * from t1,t2)b on a.c2 = b.a2;
#explain basic select count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a inner join  (select /*parallel(6)*/ * from t1,t2)b on a.c2 = b.a2;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  inner join  (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  inner join  (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  inner join  (select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  inner join  (select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;


--echo	#left join
select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t2.a1;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t2.a1;
select /*+parallel(6) use_nl(t1,t2) pq_distribute(t2 none all)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t2.a1;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t2.a1;

select /*+use_nl(t1,t2)*/ avg(t2.a2) from t1 left join t2 on t1.c2 = t2.a2;
#explain basic select /*+use_nl(t1,t2)*/ avg(t2.a2) from t1 left join t2 on t1.c2 = t2.a2;
select /*+parallel(6) use_nl(t1,t2)*/ avg(t2.a2) from t1 left join t2 on t1.c2 = t2.a2;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ avg(t2.a2) from t1 left join t2 on t1.c2 = t2.a2;

select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c2 desc;

select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 where t2.a2<50 order by t1.c2 desc;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 where t2.a2<50 order by t1.c2 desc;


select count(*) from (select * from t1 cross join t2)a left join (select * from t1,t2)b on  a.c2 = b.a2;
#explain basic select count(*) from (select * from t1 cross join t2)a left join (select * from t1,t2)b on  a.c2 = b.a2;
select /*+parallel(6)*/ count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a left join  (select /*parallel(6)*/ * from t1,t2)b on a.c2 = b.a2;
#explain basic select /*+parallel(6)*/ count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a left join  (select /*parallel(6)*/ * from t1,t2)b on a.c2 = b.a2;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  left join  (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  left join  (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
select /*+parallel(6)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select /*+parallel(6)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  left join  (select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  left join  (select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
select /*+parallel(6)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;
#explain basic select /*+parallel(6)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2;


--echo	#right join
select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;

select /*+use_nl(t1,t2)*/ avg(t2.a2) from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;
#explain basic select /*+use_nl(t1,t2)*/ avg(t2.a2) from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;
select /*+parallel(6) use_nl(t1,t2)*/ avg(t2.a2) from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ avg(t2.a2) from t1 right join t2 on t1.c2 = t2.a2 order by t2.a1;

select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c2 desc ;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
select /*+parallel(6) use_nl(t1,t2) pq_distribute(t1 none all)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c2 desc;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c2 desc;

select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;
#explain basic select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;
#explain basic select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 where t2.a2 <50 order by t1.c2 desc;

select count(*) from (select * from t1 cross join t2)a right join (select * from t1,t2)b on  a.c2 = b.a2;
#explain basic select count(*) from (select * from t1 cross join t2)a right join (select * from t1,t2)b on  a.c2 = b.a2;
select /*+parallel(6)*/ count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a right join  (select /*parallel(6)*/ * from t1,t2)b on a.c2 = b.a2;
#explain basic select /*+parallel(6)*/ count(*) from (select /*parallel(6)*/ * from t1 cross join t2)a right join  (select /*parallel(6)*/ * from t1,t2)b on a.c2 = b.a2;

select * from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  right join  (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;
#explain basic select * from (select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  right join  (select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;
select /*+parallel(6)*/ * from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;
#explain basic select /*+parallel(6)*/ * from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2)a  right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;

select * from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  right join  (select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;
#explain basic select * from (select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  right join  (select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;
select /*+parallel(6)*/ * from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;
#explain basic select /*+parallel(6)*/ * from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2)a  right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2)b on a.c2 = b.a2 order by a.c1;



