select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c1 = t2.a1 order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c1 = t2.a1 order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c1 = t2.a1 order by t1.c1;


select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c1 = t2.a1 order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 left outer join t2 on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 left outer join t2 on t1.c1 = t2.a1 order by t1.c1;



select /*+use_nl(t1,t2)*/ * from t1 full join t2 on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 full join t2 on t1.c1 = t2.a1 order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 full join t2 on t1.c1 = t2.a1 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 full join t2 on t1.c1 = t2.a1 order by t1.c1;


--echo	#半连接
--echo	#NESTED LOOP SEMI JOIN
select /*+use_nl(t1,t2)*/ * from t1  where c1 in(select a1 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c1 in(select a1 from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c1 = any(select a1 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c1 = any(select a1 from t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 in(select a1 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 in(select a1 from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 = any(select a1 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 = any(select a1 from t2) order by t1.c1;

--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/ * from t1  where c1 <(select max(a1) from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c1 <(select max(a1) from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c1 >=(select min(a1) from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c1 >=(select min(a1) from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c1 <=(select min(a1) from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c1 <=(select min(a1) from t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 <(select max(a1) from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 <(select max(a1) from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 >=(select min(a1) from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 >=(select min(a1) from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 <=(select min(a1) from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 <=(select min(a1) from t2) order by t1.c1;


--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/ * from t1  where c1 not in(select a1 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c1 not in(select a1 from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c1 != all(select a2 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c1 != all(select a2 from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is null order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 not in(select a1 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 not in(select a1 from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 != all(select a2 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c1 != all(select a2 from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is null order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c1=t2.a1 where t2.a1 is null order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c1=t2.a1 where t1.c1 is not null order by t1.c1;

--echo	#第二个类型作为连接列

select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1,t2 where t1.c2 = t2.a2 order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 inner join t2 on t1.c2 = t2.a2 order by t1.c1;

select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 left outer join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 left outer  join t2 on t1.c2 = t2.a2 order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join t2 on t1.c2 = t2.a2 order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left outer join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left outer  join t2 on t1.c2 = t2.a2 order by t1.c1;


select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 right outer join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 right outer  join t2 on t1.c2 = t2.a2 order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join t2 on t1.c2 = t2.a2 order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right outer join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right outer  join t2 on t1.c2 = t2.a2 order by t1.c1;


select /*+use_nl(t1,t2)*/ * from t1 full join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 full join t2 on t1.c2 = t2.a2 order by t1.c1;


select /*+parallel(6) use_nl(t1,t2)*/ * from t1 full join t2 on t1.c2 = t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 full join t2 on t1.c2 = t2.a2 order by t1.c1;


select /*+use_nl(t1,t2)*/ * from t1  where c2 in(select a2 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c2 in(select a2 from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c2 = any(select a2 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c2 = any(select a2 from t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 in(select a2 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 in(select a2 from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 = any(select a2 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 = any(select a2 from t2) order by t1.c1;


--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/ distinct * from t1 join t2 on t1.c2=t2.a2 order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ distinct * from t1 join t2 on t1.c2=t2.a2 order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c2 <(select max(a2) from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c2 <(select max(a2) from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c2 >=(select min(a2) from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c2 >=(select min(a2) from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c2 <=(select min(a2) from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c2 <=(select min(a2) from t2) order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ distinct * from t1 join t2 on t1.c2=t2.a2 order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ distinct * from t1 join t2 on t1.c2=t2.a2 order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 <(select max(a2) from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 <(select max(a2) from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 >=(select min(a2) from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 >=(select min(a2) from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 <=(select min(a2) from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 <=(select min(a2) from t2) order by t1.c1;


--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/ * from t1  where c2 not in(select a2 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c2 not in(select a2 from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1  where c2 != all(select a2 from t2) order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1  where c2 != all(select a2 from t2) order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c2=t2.a2 where t2.a2 is null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c2=t2.a2 where t2.a2 is null order by t1.c1;
select /*+use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c2=t2.a2 where t1.c2 is null order by t1.c1;
#explain basic  select /*+use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c2=t2.a2 where t1.c2 is null order by t1.c1;

select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 not in(select a2 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 not in(select a2 from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 != all(select a2 from t2) order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1  where c2 != all(select a2 from t2) order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c2=t2.a2 where t2.a2 is null order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 left join  t2 on t1.c2=t2.a2 where t2.a2 is null order by t1.c1;
select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c2=t2.a2 where t1.c2 is null order by t1.c1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ * from t1 right join  t2 on t1.c2=t2.a2 where t1.c2 is null order by t1.c1;



