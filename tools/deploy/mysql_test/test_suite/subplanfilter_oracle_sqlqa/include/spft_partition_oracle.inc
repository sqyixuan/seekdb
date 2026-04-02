--echo  #驱动非相关子查询
select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1;
--echo	#obs0与obs1两种模式执行计划不一致,故不打印执行计划。
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 in  (select avg(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 in  (select avg(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 in  (select avg(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 in  (select avg(a1) from t2 partition(p0)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 not in  (select avg(a1) from t2 partition(p1)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 not in  (select avg(a1) from t2 partition(p1)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 not in  (select avg(a1) from t2 partition(p1)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 not in  (select avg(a1) from t2 partition(p1)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 > any  (select avg(a1) from t2 partition(p1)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 > any  (select avg(a1) from t2 partition(p1)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 > any  (select avg(a1) from t2 partition(p1)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 > any  (select avg(a1) from t2 partition(p1)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 = all  (select avg(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 = all  (select avg(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 = all  (select avg(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 = all  (select avg(a1) from t2 partition(p0)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 <> some (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 <> some (select min(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 <> some (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 <> some (select min(a1) from t2 partition(p0)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where exists (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where exists (select min(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where exists (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where exists (select min(a1) from t2 partition(p0)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where not exists (select max(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where not exists (select max(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where not exists (select max(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where not exists (select max(a1) from t2 partition(p0)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;

select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 like (select min(a1) from t2 partition(p0)) order by c1;


select /*+no_rewrite*/ count(*) from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0)) order by t1.c1 ;
#explain basic select /*+no_rewrite*/ * from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0)) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0)) order by t1.c1 ;
#explain basic select /*+parallel(6) no_rewrite*/ * from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0)) order by t1.c1;

select /*+no_rewrite*/ count(*) from t1 cross join t2  where t1.c1 in (select min(a1) from t2) order by t1.c1;
#explain basic select /*+no_rewrite*/ count(*) from t1 cross join t2  where t1.c1 in (select min(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1 cross join t2  where t1.c1 in (select min(a1) from t2) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1 cross join t2  where t1.c1 in (select min(a1) from t2) order by t1.c1;

select /*+no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1)) order by t1.c1;
#explain basic select /*+no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1)) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1)) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1)) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any (select max(a1) from t2 partition(p0)) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any(select max(a1) from t2 partition(p0)) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any (select max(a1) from t2 partition(p0)) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any(select max(a1) from t2 partition(p0)) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0)) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0)) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0)) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0)) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1)) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1)) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1)) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1)) order by t1.c1;


select /*+no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0)) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0)) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0)) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0)) order by t1.c1;


--echo  #驱动相关子查询
select /*+no_rewrite*/ c1 from t1 partition(p1) where c1 = (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p1) where c1 = (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where c1 = (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where c1 = (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;


select /*+no_rewrite*/ c1 from t1 partition(p1) where c1 > any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p1) where c1 > any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where c1 > any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where c1 > any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p1) where c1 = all (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p1) where c1 = all (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where c1 = all (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where c1 = all (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) where c1 in (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) where c1 in (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where c1 in (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where c1 in (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) where c1 not in (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) where c1 not in (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where c1 not in (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where c1 not in (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) where  not  exists (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) where  not  exists (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where  not  exists (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) where  not  exists (select min(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ count(*) from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1 ;
#explain basic select /*+no_rewrite*/ count(*) from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0) where t1.c1 = t2.a1) order by t1.c1 ;
select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1 ;
#explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p0),t2 partition(p0) where t1.c1 = (select min(a1) from t2 partition(p0) where t1.c1 = t2.a1) order by t1.c1 ;

select /*+no_rewrite*/ count(*) from t1 partition(p1) cross join t2 partition(p1)  where t1.c1 in (select min(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ count(*) from t1 partition(p1) cross join t2 partition(p1)  where t1.c1 in (select min(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p1) cross join t2 partition(p1)  where t1.c1 in (select min(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p1) cross join t2 partition(p1)  where t1.c1 in (select min(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;

select /*+no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1 partition(p1) natural join t2 partition(p1) where t1.c1 = any (select avg(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p1),t2 partition(p1) where t1.c1 = t2.a1 and t1.c1 < any (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) inner join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 < some (select max(a1) from t2 partition(p1) where t2.a1 =t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) left join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 = all (select avg(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where exists (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;

select /*+no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 like (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 like (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 like (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;
#explain basic select /*+parallel(6) no_rewrite*/ c1 from t1 partition(p0) right join t2 partition(p0) on t1.c1 =t2.a1 where t1.c1 like (select avg(a1) from t2 partition(p0) where t2.a1 =t1.c1) order by t1.c1;

insert into t1 partition(p0)  select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1 fetch next 1 rows only;
#explain insert into t1 partition(p0)  select /*+no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1 fetch next 1 rows only;
insert into t1 partition(p0)  select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1 fetch next 1 rows only;
#explain insert into t1 partition(p0)  select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where t1.c1 > (select min(a1) from t2 partition(p0)) order by c1 fetch next 1 rows only;

insert into t1 partition(p0) select /*+no_rewrite*/ * from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;
#explain insert into t1 partition(p0) select /*+no_rewrite*/ * from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;
insert into t1 partition(p0) select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;
#explain insert into t1 partition(p0) select /*+parallel(6) no_rewrite*/ * from t1 partition(p0) where c1 = some (select max(a1) from t2 partition(p0) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;


insert into t1 partition(p1) select /*+no_rewrite*/ * from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;
#explain insert into t1 partition(p1) select /*+no_rewrite*/ * from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;
insert into t1 partition(p1) select /*+parallel(6) no_rewrite*/ * from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;
#explain insert into t1 partition(p1) select /*+parallel(6) no_rewrite*/ * from t1 partition(p1) where exists (select max(a1) from t2 partition(p1) where t2.a1 = t1.c1) order by t1.c1 fetch next 1 rows only;


