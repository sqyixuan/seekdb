
--echo	#无条件连接
select count(*) from t1,t2 partition(p0);
#explain basic  select count(*)  from t1,t2 partition(p0);
select count(*)  from t1 NATURAL join t2 partition(p1);
#explain basic  select count(*)  from t1 NATURAL join t2 partition(p1);
select count(*)  from t1 cross join t2 partition(p2);
#explain basic  select count(*)  from t1 cross join t2 partition(p2);

select /*+parallel(6)*/ count(*)  from t1,t2 partition(p0);
#explain basic  select /*+parallel(6)*/ count(*)  from t1,t2 partition(p0);
select /*+parallel(6)*/ count(*)  from t1 NATURAL join t2 partition(p1);
#explain basic  select /*+parallel(6)*/ count(*)  from t1 NATURAL join t2 partition(p1);
select /*+parallel(6)*/ count(*)  from t1 cross join t2 partition(p2);
#explain basic  select /*+parallel(6)*/ count(*)  from t1 cross join t2 partition(p2);


select count(*)  from t2 partition(p1),t1;
#explain basic  select count(*)  from t2 partition(p1),t1;
select /*+parallel(6)*/ count(*)  from t2 partition(p1),t1;
#explain basic  select /*+parallel(6)*/ count(*)  from t2 partition(p1),t1;
select count(*)  from t2 partition(p0) NATURAL join t1;
#explain basic  select count(*)  from t2 partition(p0) NATURAL join t1;
select /*+parallel(6)*/ count(*)  from t2 partition(p0) NATURAL join t1;
#explain basic  select /*+parallel(6)*/ count(*)  from t2 partition(p0) NATURAL join t1;
select count(*)  from t2 partition(p1) cross join t1;
#explain basic  select count(*)  from t2 partition(p1) cross join t1;
select /*+parallel(6)*/ count(*)  from t2 partition(p1) cross join t1;
#explain basic  select /*+parallel(6)*/ count(*)  from t2 partition(p1) cross join t1;


--echo	#基本链接
select count(*)  from t1,t2 partition(p0) where t1.c1 = t2.a1;
#explain basic  select count(*)  from t1,t2 partition(p0) where t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/ count(*)  from t1,t2 partition(p0) where t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1,t2 partition(p0) where t1.c1 = t2.a1;
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1,t2 partition(p0) where t1.c1 = t2.a1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1,t2 partition(p0) where t1.c1 = t2.a1;
select count(*)  from t1 inner join t2 partition(p1) on t1.c1 = t2.a1;
#explain basic  select count(*)  from t1 inner join t2 partition(p1) on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/ count(*)  from t1 inner join t2 partition(p1) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1 inner join t2 partition(p1) on t1.c1 = t2.a1;
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1 inner join t2 partition(p1) on t1.c1 = t2.a1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1 inner join t2 partition(p1) on t1.c1 = t2.a1;



select count(*)  from t2 partition(p1),t1 where t2.a2 = t1.c2;
#explain basic  select count(*)  from t2 partition(p1),t1 where t2.a2 = t1.c2;
select /*+use_nl(t2,t1)*/ count(*)  from t2 partition(p1),t1 where t2.a2 = t1.c2;
#explain basic  select /*+use_nl(t2,t1)*/ count(*)  from t2 partition(p1),t1 where t2.a2 = t1.c2;
select /*+parallel(4) use_nl(t2,t1)*/ count(*)  from t2 partition(p1),t1 where t2.a2 = t1.c2;
#explain basic  select /*+parallel(4) use_nl(t2,t1)*/ count(*)  from t2 partition(p1),t1 where t2.a2 = t1.c2;
select count(*)  from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2;
#explain basic  select count(*)  from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2;
select /*+use_nl(t2,t1)*/ count(*)  from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2;
#explain basic  select /*+use_nl(t2,t1)*/ count(*)  from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2;
select /*+parallel(4) use_nl(t2,t1)*/ count(*)  from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2;
#explain basic  select /*+parallel(4) use_nl(t2,t1)*/ count(*)  from t2 partition(p1) inner join  t1 on t2.a2 = t1.c2;


--echo	#left join
select count(*)  from t1 left join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select count(*)  from t1 left join t2 partition(p2) on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/ count(*)  from t1 left join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1 left join t2 partition(p2) on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/ count(*)  from t1 left outer join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1 left outer join t2 partition(p2) on t1.c1 = t2.a1;
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1 left join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1 left join t2 partition(p2) on t1.c1 = t2.a1;





--echo	#right join
select count(*)  from t1 right join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select count(*)  from t1 right join t2 partition(p2) on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/ count(*)  from t1 right join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1 right join t2 partition(p2) on t1.c1 = t2.a1;
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1 right join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1 right join t2 partition(p2) on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/ count(*)  from t1 right outer join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1 right outer join t2 partition(p2) on t1.c1 = t2.a1;



--echo	#半连接
--echo	#NESTED LOOP SEMI JOIN
select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 in(select a1 from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 in(select a1 from t2 partition(p1));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 in(select a1 from t2 partition(p1));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 in(select a1 from t2 partition(p1));

select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 = any(select a1 from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 = any(select a1 from t2 partition(p2));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 = any(select a1 from t2 partition(p2));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 = any(select a1 from t2 partition(p2));


--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 >(select avg(a1) from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 >(select avg(a1) from t2 partition(p1));
select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 <(select max(a1) from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from  t1  where c1 <(select max(a1) from t2 partition(p1));
select /*+use_nl(t1,t2)*/ count(*)  from  t1  where c1 >=(select min(a1) from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 >=(select min(a1) from t2 partition(p2));
select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 <=(select min(a1) from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 <=(select min(a1) from t2 partition(p1));
select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 !=(select avg(a1) from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 !=(select avg(a1) from t2 partition(p2));
select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 <> (select avg(a1) from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from  t1   where c1 <>(select avg(a1) from t2 partition(p2));

select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 >(select avg(a1) from t2 partition(p1));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 >(select avg(a1) from t2 partition(p1));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 <(select max(a1) from t2 partition(p1));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1  where c1 <(select max(a1) from t2 partition(p1));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1  where c1 >=(select min(a1) from t2 partition(p2));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 >=(select min(a1) from t2 partition(p2));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 <=(select min(a1) from t2 partition(p1));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 <=(select min(a1) from t2 partition(p1));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 !=(select avg(a1) from t2 partition(p2));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 !=(select avg(a1) from t2 partition(p2));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 <> (select avg(a1) from t2 partition(p2));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from  t1   where c1 <>(select avg(a1) from t2 partition(p2));

--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 not in(select a1 from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 not in(select a1 from t2 partition(p2));
select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 != all(select a1 from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1   where c1 != all(select a1 from t2 partition(p2));

select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 not in(select a1 from t2 partition(p2));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 not in(select a1 from t2 partition(p2));
select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 != all(select a1 from t2 partition(p2));
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*)  from t1   where c1 != all(select a1 from t2 partition(p2));

select /*+use_nl(t1,t2)*/ count(*)  from t1  left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1  left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null;
select /*+use_nl(t1,t2)*/ count(*)  from t1  right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null;
#explain basic  select /*+use_nl(t1,t2)*/ count(*)  from t1  right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null;


