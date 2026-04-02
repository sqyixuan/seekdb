select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p1),t2 partition(p1);
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p1),t2 partition(p1);

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2) NATURAL JOIN t2 partition(p3);
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2) NATURAL JOIN t2 partition(p3);

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p3) cross join t2 partition(p2);
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p3) cross join t2 partition(p2);



--echo	#inner

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1;
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1;

select  /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2) inner join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select  /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2) inner join t2 partition(p2) on t1.c1 = t2.a1;
select  /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2) inner join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select  /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2) inner join t2 partition(p2) on t1.c1 = t2.a1;



--echo #left join

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1;
select /*+parallel(4) use_nl(t1,t2)*/ count(*) from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1;
#explain basic  select /*+parallel(4) use_nl(t1,t2)*/ count(*) from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1;


--echo	#right join

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1;
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1;


--echo	#full join

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p1) full join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p1) full join t2 partition(p2) on t1.c1 = t2.a1;
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p1) full join t2 partition(p2) on t1.c1 = t2.a1;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p1) full join t2 partition(p2) on t1.c1 = t2.a1;



--echo	#半连接
--echo	#NESTED LOOP SEMI JOIN
select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1));

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 = any(select a1 from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 = any(select a1 from t2 partition(p2));

select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1));
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1));

select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 = any(select a1 from t2 partition(p2));
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 = any(select a1 from t2 partition(p2));

--echo	#NESTED LOOP JOIN
select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1));

select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p1)  where c1 <(select max(a1) from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p1)  where c1 <(select max(a1) from t2 partition(p1));

select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p2) where c1 >=(select min(a1) from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p2)  where c1 >=(select min(a1) from t2 partition(p2));

select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p1)  where c1 <=(select min(a1) from t2 partition(p1));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p1)  where c1 <=(select min(a1) from t2 partition(p1));

select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p3)  where c1 !=(select avg(a1) from t2 partition(p3));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p3)  where c1 !=(select avg(a1) from t2 partition(p3));

select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p2)  where c1 <> (select avg(a1) from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from  t1 partition(p2)  where c1 <>(select avg(a1) from t2 partition(p2));


--echo	#NESTED-LOOP ANTI JOIN
select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2));
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2));
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2));

select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 != all(select a1 from t2 partition(p2));
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 != all(select a1 from t2 partition(p2));
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 != all(select a1 from t2 partition(p2));
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2)  where c1 != all(select a1 from t2 partition(p2));


--echo	#nested loop join 不加分区前为 nested loop anti join
select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2) left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null;
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2) left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null;
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2) left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2) left join  t2 partition(p2) on t1.c1=t2.a1 where t2.a1 is not null;


select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null;
#explain basic  select /*+use_nl(t1,t2)*/ count(*) from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null;
select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null;
#explain basic  select /*+parallel(6) use_nl(t1,t2)*/ count(*) from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null;


--echo	#无条件链接嵌套
select count(*) from (select * from t1 partition(p1),t2 partition(p1))a,(select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;
#explain basic  select count(*) from (select * from t1 partition(p1),t2 partition(p1))a,(select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;

select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;

select count(*) from (select /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a,(select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a,(select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;

select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;


--echo	#NATURAL JOIN 嵌套
select count(*) from (select * from t1 partition(p1),t2 partition(p1))a natural join (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;
#explain basic  select count(*) from (select * from t1 partition(p1),t2 partition(p1))a natural join (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;

select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a natural join (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a natural join (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a natural join (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a natural join (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a natural join (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a natural join (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a natural join (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a natural join (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;


select count(*) from (select /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a natural join (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a natural join (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;

select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a natural join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;


--echo	#CROSS JOIN 嵌套
select count(*) from (select * from t1 partition(p1),t2 partition(p1))a cross join (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;
#explain basic  select count(*) from (select * from t1 partition(p1),t2 partition(p1))a cross join (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;

select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a cross join (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a cross join (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a cross join (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a cross join (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a cross join (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a cross join (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;

select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a cross join (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a cross join (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;


select count(*) from (select /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a cross join (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a cross join (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b;

select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b;

select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a cross join (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null);






--echo #基本链接嵌套
select count(*) from (select * from t1 partition(p1),t2 partition(p1))a,(select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b where a.c2 = b.a2;
#explain basic  select count(*) from (select * from t1 partition(p1),t2 partition(p1))a,(select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b where a.c2 = b.a2;
select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b where a.c2 = b.a2;
#explain basic  select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b where a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b where a.c2 = b.a2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b where a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b where a.c2 = b.c2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b where a.c2 = b.c2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b where a.c2 = b.c2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b where a.c2 = b.c2;

select count(*) from (select /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a,(select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b where a.c2 = b.a2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select  /*+parallel(6),READ_CONSISTENCY (weak)*/ * from t1 partition(p1),t2 partition(p1))a,(select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b where a.c2 = b.a2;
select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b where a.c2 = b.a2;
#explain basic  select count(*) from (select  /*+parallel(6),READ_CONSISTENCY (weak)*/ * from t1 partition(p3) cross join t2 partition(p2))a,(select /*+parallel(6) use_nl(t1,t2),READ_CONSISTENCY (weak)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b where a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b where a.c2 = b.a2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2),READ_CONSISTENCY (weak)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a,(select /*+parallel(6) use_nl(t1,t2),READ_CONSISTENCY (weak)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b where a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b where a.c2 = b.c2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b where a.c2 = b.c2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b where a.c2 = b.c2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a,(select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b where a.c2 = b.c2;


select count(*) from (select * from t1 partition(p1),t2 partition(p1))a  inner join   (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select * from t1 partition(p1),t2 partition(p1))a inner join  (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a inner join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a inner join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a inner join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a inner join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a inner join  (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
#explain basic  select /*+READ_CONSISTENCY (weak)*/ count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a inner join  (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a inner join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a inner join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;

select count(*) from (select /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a inner join  (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a inner join  (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select  /*+parallel(6),READ_CONSISTENCY (weak)*/ * from t1 partition(p3) cross join t2 partition(p2))a inner join  (select /*+parallel(6) use_nl(t1,t2),READ_CONSISTENCY (weak)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a inner join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;



--echo	#左连接嵌套
select count(*) from (select * from t1 partition(p1),t2 partition(p1))a  left join   (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
#explain basic  select count(*) from (select * from t1 partition(p1),t2 partition(p1))a left join  (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a left join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a left join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a left join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a left join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a left join  (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a left join  (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a left join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a left join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;

select count(*) from (select /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a left join  (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a left join  (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;
#explain basic  select * from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a left join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;




--echo	#右连接
select count(*) from (select * from t1 partition(p1),t2 partition(p1))a  right join   (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
#explain basic  select * from (select * from t1 partition(p1),t2 partition(p1))a right join  (select * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a right join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select * from t1 partition(p3) cross join t2 partition(p2))a right join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a right join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a right join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a right join  (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a right join  (select /*+use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a right join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a right join  (select /*+use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;

select count(*) from (select /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a right join  (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p1),t2 partition(p1))a right join  (select  /*+parallel(6)*/ * from t1 partition(p2) NATURAL JOIN t2 partition(p3))b on a.c2 = b.a2;
select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select  /*+parallel(6)*/ * from t1 partition(p3) cross join t2 partition(p2))a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) left join t2 partition(p1) on t1.c1 = t2.a1)a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p3) right join t2 partition(p1) on t1.c1 = t2.a1)b on a.c2 = b.a2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p1)  where c1 in(select a1 from t2 partition(p1)))a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from  t1 partition(p1)  where c1 >(select avg(a1) from t2 partition(p1)))b on a.c2 = b.c2;
select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;
#explain basic  select count(*) from (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2)  where c1 not in(select a1 from t2 partition(p2)))a right join  (select /*+parallel(6) use_nl(t1,t2)*/ * from t1 partition(p2) right join  t2 partition(p2) on t1.c1=t2.a1 where t1.c1 is not null)b on a.c2 = b.c2;



