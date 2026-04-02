--echo	#无条件指定连接
select count(*) from t1,t2;
#explain basic  select count(*) from t1,t2;
select /*+use_nl(t1,t2)*/count(*) from t1,t2;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1,t2;
select count(*) from t1 natural join t2;
#explain basic  select count(*) from t1 natural join t2;
select /*+use_nl(t1,t2)*/count(*) from t1 natural join t2;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 natural join t2;
select count(*) from t1 cross join t2;
#explain basic  select count(*) from t1 cross join t2;
select /*+use_nl(t1,t2)*/count(*) from t1 cross join t2;
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 cross join t2;


--echo	#inner join
select count(*) from t1,t2 where t1.c1 = t2.a1 ;
#explain basic  select count(*) from t1,t2 where t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1,t2 where t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1,t2 where t1.c1 = t2.a1;
select count(*) from t1 inner join t2 on t1.c1 = t2.a1;
#explain basic  select count(*) from t1 inner join t2 on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1 inner join t2 on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 inner join t2 on t1.c1 = t2.a1;

--echo	#left join
select count(*) from t1 left join t2 on t1.c1 = t2.a1;
#explain basic  select count(*) from t1 left join t2 on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1 left join t2 on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 left join t2 on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1 left outer join t2 on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 left outer join t2 on t1.c1 = t2.a1;
select count(*) from t1,t2 where t1.c1 = t2.a1(+);
#explain basic  select count(*) from t1,t2 where t1.c1 = t2.a1(+);
select /*+use_nl(t1,t2)*/count(*) from t1,t2 where t1.c1 = t2.a1(+);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1,t2 where t1.c1 = t2.a1(+);


--echo	#right join
select count(*) from t1 right join t2 on t1.c1 = t2.a1;
#explain basic  select count(*) from t1 right join t2 on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1 right join t2 on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 right join t2 on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1 right outer join t2 on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 right outer join t2 on t1.c1 = t2.a1;
select count(*) from t1,t2 where t1.c1(+) = t2.a1;
#explain basic  select count(*) from t1,t2 where t1.c1(+) = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1,t2 where t1.c1(+) = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1,t2 where t1.c1(+) = t2.a1;


--echo	#full join
select count(*) from t1 full join t2 on t1.c1 = t2.a1;
#explain basic  select count(*) from t1 full join t2 on t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1 full join t2 on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 full join t2 on t1.c1 = t2.a1;


--echo	#半连接
--echo	#NESTED LOOP SEMI JOIN
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 in(select a1 from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 in(select a1 from t2);
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 = any(select a1 from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 = any(select a1 from t2);
--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/distinct * from t1 join t2 on t1.c1=t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/distinct * from t1 join t2 on t1.c1=t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 >(select avg(a1) from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 >(select avg(a1) from t2);
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 <(select max(a1) from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 <(select max(a1) from t2);
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 >=(select min(a1) from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 >=(select min(a1) from t2);
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 <=(select min(a1) from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 <=(select min(a1) from t2);
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 !=(select avg(a1) from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 !=(select avg(a1) from t2);
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 <> (select avg(a1) from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 <>(select avg(a1) from t2);

--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 not in(select a1 from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 not in(select a1 from t2);
select /*+use_nl(t1,t2)*/count(*) from t1  where c1 != all(select a1 from t2);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1  where c1 != all(select a1 from t2);

select /*+use_nl(t1,t2)*/count(*) from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is not null;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is not null;
select /*+use_nl(t1,t2)*/count(*) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is not null;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is not null;



--echo #两个组合分区表进行连接
--echo	#无条件连接
select  count(*) from t1 partition(p1),t2 partition(p1);
#explain basic  select count(*) from t1 partition(p1),t2 partition(p1);
select /*+use_nl(t1,t2)*/count(*) from t1 partition(p1),t2 partition(p1);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 partition(p1),t2 partition(p1);
select count(*) from t1 partition(p0) NATURAL JOIN t2 partition(p0);
#explain basic  select count(*) from t1 partition(p0) NATURAL JOIN t2 partition(p0);
select /*+use_nl(t1,t2)*/count(*) from t1 partition(p0) NATURAL JOIN t2 partition(p0);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 partition(p0) NATURAL JOIN t2 partition(p0);
select count(*) from t1 partition(p0) cross join t2 partition(p0);
#explain basic  select count(*) from t1 partition(p0) cross join t2 partition(p0);
select /*+use_nl(t1,t2)*/count(*) from t1 partition(p0) cross join t2 partition(p0);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 partition(p0) cross join t2 partition(p0);

--echo	#inner
select * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 order by t1.c1;
select * from t1 partition(p0) inner join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(p0) inner join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;
select  /*+use_nl(t1,t2)*/* from t1 partition(p0) inner join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select  /*+use_nl(t1,t2)*/* from t1 partition(p0) inner join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;


--echo #left join
select * from t1 partition(p0) left join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(p0) left join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p0) left join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0) left join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p0) left outer join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0) left outer join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
select * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1(+) order by t1.c1;
#explain basic  select * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1(+) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1(+) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1(+) order by t1.c1;

--echo	#right join
select * from t1 partition(p0) right join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(p0) right join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p0) right join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0) right join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p1) right outer join t2 partition(p1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p1) right outer join t2 partition(p1) on t1.c1 = t2.a1 ;
select * from t1 partition(p0),t2 partition(p1) where t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(p0),t2 partition(p1) where t1.c1(+) = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p0),t2 partition(p1) where t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0),t2 partition(p1) where t1.c1(+) = t2.a1 order by t1.c1;

--echo	#full join
select * from t1 partition(p1) full join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(p1) full join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p1) full join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p1) full join t2 partition(p0) on t1.c1 = t2.a1 order by t1.c1;

--echo	#半连接
--echo	#NESTED LOOP SEMI JOIN
select /*+use_nl(t1,t2)*/* from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p0)  where c1 = any(select a1 from t2 partition(p0)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0)  where c1 = any(select a1 from t2 partition(p0)) order by t1.c1;

--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/distinct * from  t1 partition(p1) join t2 partition(p1) on t1.c1=t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/distinct * from  t1 partition(p1) join t2 partition(p1) on t1.c1=t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(p1)  where c1 <(select max(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(p1)  where c1 <(select max(a1) from t2 partition(p1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(p0) where c1 >=(select min(a1) from t2 partition(p0)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(p0)  where c1 >=(select min(a1) from t2 partition(p0)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(p1)  where c1 <=(select min(a1) from t2 partition(p1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(p1)  where c1 <=(select min(a1) from t2 partition(p1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(p0)  where c1 !=(select avg(a1) from t2 partition(p0)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(p0)  where c1 !=(select avg(a1) from t2 partition(p0)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(p0)  where c1 <> (select avg(a1) from t2 partition(p0)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(p0)  where c1 <>(select avg(a1) from t2 partition(p0)) order by t1.c1;

--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/* from t1 partition(p0)  where c1 not in(select a1 from t2 partition(p0)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0)  where c1 not in(select a1 from t2 partition(p0)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p0)  where c1 != all(select a1 from t2 partition(p2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0)  where c1 != all(select a1 from t2 partition(p0)) order by t1.c1;

select /*+use_nl(t1,t2)*/* from t1 partition(p0) left join  t2 partition(p0) on t1.c1=t2.a1 where t2.a1 is not null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0) left join  t2 partition(p0) on t1.c1=t2.a1 where t2.a1 is not null order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(p0) right join  t2 partition(p0) on t1.c1=t2.a1 where t1.c1 is not null;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(p0) right join  t2 partition(p0) on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;



--echo	#两个组合分区内表进行组合
--echo	#无条件连接
select count(*) from t1 partition(sp1),t2 partition(sp1);
#explain basic  select count(*) from t1 partition(sp1),t2 partition(sp1);
select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp1),t2 partition(sp1);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp1),t2 partition(sp1);
select count(*) from t1 partition(sp1) NATURAL JOIN t2 partition(sp1);
#explain basic  select count(*) from t1 partition(sp1) NATURAL JOIN t2 partition(sp1);
select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp1) NATURAL JOIN t2 partition(sp1);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp1) NATURAL JOIN t2 partition(sp1);
select count(*) from t1 partition(sp1) cross join t2 partition(sp1);
#explain basic  select count(*) from t1 partition(sp1) cross join t2 partition(sp1);
select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp1) cross join t2 partition(sp1);
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp1) cross join t2 partition(sp1);

--echo	#inner
select count(*) from t1 partition(sp2),t2 partition(sp2) where t1.c1 = t2.a1;
#explain basic  select count(*) from t1 partition(sp2),t2 partition(sp2) where t1.c1 = t2.a1;
select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp2),t2 partition(sp2) where t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/count(*) from t1 partition(sp2),t2 partition(sp2) where t1.c1 = t2.a1;
select count(*) from t1 partition(sp2) inner join t2 partition(sp2) on t1.c1 = t2.a1;
#explain basic  select count(*) from t1 partition(sp2) inner join t2 partition(sp2) on t1.c1 = t2.a1;
select  /*+use_nl(t1,t2)*/count(*) from t1 partition(sp2) inner join t2 partition(sp2) on t1.c1 = t2.a1;
#explain basic  select  /*+use_nl(t1,t2)*/count(*) from t1 partition(sp2) inner join t2 partition(sp2) on t1.c1 = t2.a1;


--echo #left join
select * from t1 partition(sp0) left join t2 partition(sp0) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(sp0) left join t2 partition(sp0) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp0) left join t2 partition(sp0) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp0) left join t2 partition(sp0) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp0) left outer join t2 partition(sp0) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp0) left outer join t2 partition(sp0) on t1.c1 = t2.a1 order by t1.c1;
select * from t1 partition(sp0),t2 partition(sp0) where t1.c1 = t2.a1(+) order by t1.c1;
#explain basic  select * from t1 partition(sp0),t2 partition(sp0) where t1.c1 = t2.a1(+) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp0),t2 partition(sp0) where t1.c1 = t2.a1(+) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp0),t2 partition(sp0) where t1.c1 = t2.a1(+) order by t1.c1;

--echo	#right join
select * from t1 partition(sp1) right join t2 partition(sp1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(sp1) right join t2 partition(sp1) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp2) right join t2 partition(sp2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp2) right join t2 partition(sp2) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp2) right outer join t2 partition(sp2) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp2) right outer join t2 partition(sp2) on t1.c1 = t2.a1 order by t1.c1;
select * from t1 partition(sp2),t2 partition(sp2) where t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(sp2),t2 partition(sp2) where t1.c1(+) = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp1),t2 partition(sp1) where t1.c1(+) = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp1),t2 partition(sp1) where t1.c1(+) = t2.a1 order by t1.c1;

--echo	#full join
select * from t1 partition(sp1) full join t2 partition(sp1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select * from t1 partition(sp1) full join t2 partition(sp1) on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp1) full join t2 partition(sp1) on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp1) full join t2 partition(sp1) on t1.c1 = t2.a1 order by t1.c1;

--echo	#半连接
--echo	#NESTED LOOP SEMI JOIN
select /*+use_nl(t1,t2)*/* from t1 partition(sp1)  where c1 in(select a1 from t2 partition(sp1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp1)  where c1 in(select a1 from t2 partition(sp1)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp1)  where c1 = any(select a1 from t2 partition(sp1)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp1)  where c1 = any(select a1 from t2 partition(sp1)) order by t1.c1;

--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/distinct * from  t1 partition(sp2) join t2 partition(sp2) on t1.c1=t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/distinct * from  t1 partition(sp2) join t2 partition(sp2) on t1.c1=t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 >(select avg(a1) from t2 partition(sp2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 >(select avg(a1) from t2 partition(sp2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 <(select max(a1) from t2 partition(sp2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 <(select max(a1) from t2 partition(sp2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(sp2) where c1 >=(select min(a1) from t2 partition(sp2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 >=(select min(a1) from t2 partition(sp2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 <=(select min(a1) from t2 partition(sp2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 <=(select min(a1) from t2 partition(sp2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 !=(select avg(a1) from t2 partition(sp2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 !=(select avg(a1) from t2 partition(sp2)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 <> (select avg(a1) from t2 partition(sp2)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from  t1 partition(sp2)  where c1 <>(select avg(a1) from t2 partition(sp2)) order by t1.c1;

--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/* from t1 partition(sp0)  where c1 not in(select a1 from t2 partition(sp0)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp0)  where c1 not in(select a1 from t2 partition(sp0)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp0)  where c1 != all(select a1 from t2 partition(sp0)) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp0)  where c1 != all(select a1 from t2 partition(sp0)) order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp1) left join  t2 partition(sp1) on t1.c1=t2.a1 where t2.a1 is not null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp1) left join  t2 partition(sp1) on t1.c1=t2.a1 where t2.a1 is not null order by t1.c1;
select /*+use_nl(t1,t2)*/* from t1 partition(sp1) right join  t2 partition(sp1) on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/* from t1 partition(sp1) right join  t2 partition(sp1) on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;


