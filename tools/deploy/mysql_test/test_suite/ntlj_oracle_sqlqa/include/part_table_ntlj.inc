--echo	#无条件连接
select count(*) from t1,t2 partition(p0);
#explain basic  select count(*) from t1,t2 partition(p0);
select count(*) from t1 NATURAL join t2 partition(p1);
#explain basic  select count(*) from t1 NATURAL join t2 partition(p1);
select count(*) from t1 cross join t2 partition(p2);
#explain basic  select count(*) from t1 cross join t2 partition(p2);

select /*+parallel(6)*/ count(*) from t1,t2 partition(p0);
#explain basic  select /*+parallel(6)*/ count(*) from t1,t2 partition(p0);
select /*+parallel(6)*/ count(*) from t1 NATURAL join t2 partition(p1);
#explain basic  select /*+parallel(6)*/ count(*) from t1 NATURAL join t2 partition(p1);
select /*+parallel(6)*/ count(*) from t1 cross join t2 partition(p2);
#explain basic  select /*+parallel(6)*/ count(*) from t1 cross join t2 partition(p2);


select count(*) from t2 partition(p1),t1;
#explain basic  select count(*) from t2 partition(p1),t1;
select /*+parallel(6)*/ count(*) from t2 partition(p1),t1;
#explain basic  select /*+parallel(6)*/ count(*) from t2 partition(p1),t1;
select count(*) from t2 partition(p0) NATURAL join t1;
#explain basic  select count(*) from t2 partition(p0) NATURAL join t1;
select /*+parallel(6)*/ count(*) from t2 partition(p0) NATURAL join t1;
#explain basic  select /*+parallel(6)*/ count(*) from t2 partition(p0) NATURAL join t1;
select count(*) from t2 partition(p1) cross join t1;
#explain basic  select count(*) from t2 partition(p1) cross join t1;
select /*+parallel(6)*/ count(*) from t2 partition(p1) cross join t1;
#explain basic  select /*+parallel(6)*/ count(*) from t2 partition(p1) cross join t1;


--echo	#基本链接
select * from t1,t2 partition(p0) where t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1,t2 partition(p0) where t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1,t2 partition(p0) where t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1,t2 partition(p0) where t1.c1 = t2.a1 order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1,t2 partition(p0) where t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1,t2 partition(p0) where t1.c1 = t2.a1 order by t1.c1;
select * from t1 inner join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 inner join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 inner join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 inner join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1 inner join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1 inner join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;



select * from t2 partition(p1),t1 where t2.a2 = t1.c2 order by t1.c1;
#explain basic  select * from t2 partition(p1),t1 where t2.a2 = t1.c2 order by t1.c1;
select /*+use_nl(t2,t1)*/ * from t2 partition(p1),t1 where t2.a2 = t1.c2 order by t1.c1;
#explain basic  select /*+use_nl(t2,t1)*/ * from t2 partition(p1),t1 where t2.a2 = t1.c2 order by t1.c1;
select /*+parallel(4) use_nl(t2,t1)*/ * from t2 partition(p1),t1 where t2.a2 = t1.c2 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t2,t1)*/ * from t2 partition(p1),t1 where t2.a2 = t1.c2 order by t1.c1;
select * from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2 order by t1.c1;
#explain basic  select * from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2 order by t1.c1;
select /*+use_nl(t2,t1)*/ * from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2 order by t1.c1;
#explain basic  select /*+use_nl(t2,t1)*/ * from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2 order by t1.c1;
select /*+parallel(4) use_nl(t2,t1)*/ * from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t2,t1)*/ * from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2 order by t1.c1;


--echo	#left join 
select * from t1 left join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 left join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 left join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 left join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 left outer join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 left outer join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1 left join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1 left join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;

select * from t1,t2 partition(p2) where t1.c1 = t2.a1(+) order by t1.c1;
#explain basic  select * from t1,t2 partition(p2) where t1.c1 = t2.a1(+) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1 = t2.a1(+) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1 = t2.a1(+) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1 = t2.a1(+) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1 = t2.a1(+) order by t1.c1;




--echo	#right join
select * from t1 right join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 right join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 right join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 right join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1 right join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1 right join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 right outer join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 right outer join t2 partition(p2) on t1.c1 = t2.a1 order by t1.c1;
select * from t1,t2 partition(p2) where t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  select * from t1,t2 partition(p2) where t1.c1(+) = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1(+) = t2.a1 order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1,t2 partition(p2) where t1.c1(+) = t2.a1 order by t1.c1;


--echo	#半连接
--echo	#NESTED LOOP SEMI JOIN
select /*+use_nl(t1,t2)*/* from t1   where c1 in(select a1 from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1   where c1 in(select a1 from t2 partition(p1)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 in(select a1 from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 in(select a1 from t2 partition(p1)) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1   where c1 = any(select a1 from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1   where c1 = any(select a1 from t2 partition(p2)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 = any(select a1 from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 = any(select a1 from t2 partition(p2)) order by t1.c1;


--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/distinct * from  t1  join t2 partition(p1) on t1.c1=t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/distinct * from  t1  join t2 partition(p1) on t1.c1=t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1   where c1 >(select avg(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1   where c1 >(select avg(a1) from t2 partition(p1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1   where c1 <(select max(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1  where c1 <(select max(a1) from t2 partition(p1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1  where c1 >=(select min(a1) from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1   where c1 >=(select min(a1) from t2 partition(p2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1   where c1 <=(select min(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1   where c1 <=(select min(a1) from t2 partition(p1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1   where c1 !=(select avg(a1) from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1   where c1 !=(select avg(a1) from t2 partition(p2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1   where c1 <> (select avg(a1) from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1   where c1 <>(select avg(a1) from t2 partition(p2)) order by t1.c1;

select /*+parallel(4) use_nl(t1,t2)*/distinct * from  t1  join t2 partition(p1) on t1.c1=t2.a1 order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/distinct * from  t1  join t2 partition(p1) on t1.c1=t2.a1 order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 >(select avg(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 >(select avg(a1) from t2 partition(p1)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 <(select max(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from  t1  where c1 <(select max(a1) from t2 partition(p1)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from  t1  where c1 >=(select min(a1) from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 >=(select min(a1) from t2 partition(p2)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 <=(select min(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 <=(select min(a1) from t2 partition(p1)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 !=(select avg(a1) from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 !=(select avg(a1) from t2 partition(p2)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 <> (select avg(a1) from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from  t1   where c1 <>(select avg(a1) from t2 partition(p2)) order by t1.c1;

--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/* from t1   where c1 not in(select a1 from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1   where c1 not in(select a1 from t2 partition(p2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1   where c1 != all(select a1 from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1   where c1 != all(select a1 from t2 partition(p2)) order by t1.c1;

select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 not in(select a1 from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 not in(select a1 from t2 partition(p2)) order by t1.c1;
select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 != all(select a1 from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/* from t1   where c1 != all(select a1 from t2 partition(p2)) order by t1.c1;

--echo	#nested loop join 不加分区前为 nested loop anti join
select /*+use_nl(t1,t2)*/* from t1  left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1  right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1  right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;


