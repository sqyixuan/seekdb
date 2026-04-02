select /*+no_rewrite*/ * from t1 where c1 = (select max(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 = (select max(a1) from t2) order by c1 ;

select /*+no_rewrite*/ * from t1 where c1 < (select min(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 < (select min(a1) from t2) order by c1 ;

select /*+no_rewrite*/ * from t1 where c1 >= (select max(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 >= (select max(a1) from t2) order by c1 ;

select /*+no_rewrite*/ * from t1 where c1 <= (select count(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 <= (select count(a1) from t2) order by c1 ;

select /*+no_rewrite*/ * from t1 where c1 <> (select min(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 <> (select min(a1) from t2) order by c1 ;

select /*+no_rewrite*/ * from t1 where c1 != (select max(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 != (select max(a1) from t2) order by c1 ;


--echo  #in
select /*+no_rewrite*/ count(c1) from t1 where c1 in (select max(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ count(c1) from t1 where c1 in (select max(a1) from t2) order by c1 ;


select /*+no_rewrite*/ max(c1) from t1 where c1 not in (select min(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ max(c1) from t1 where c1 not in (select min(a1) from t2) order by c1 ;

--echo  #all
select /*+no_rewrite*/ min(c1) from t1 where c1 < all (select max(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ min(c1) from t1 where c1 < all (select max(a1) from t2) order by c1 ;

--echo  #any
select /*+no_rewrite*/ avg(c1) from t1 where c1 = any (select min(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ avg(c1) from t1 where c1 = any (select min(a1) from t2) order by c1 ;

--echo  #some
select /*+no_rewrite*/ count(c1) from t1 where c1 > some (select min(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ count(c1) from t1 where c1 > some (select min(a1) from t2) order by c1 ;

--echo  #exists
select /*+no_rewrite*/ * from t1 where exists (select min(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where exists (select min(a1) from t2) order by c1 ;

--echo  #not exists
select /*+no_rewrite*/ * from t1 where not exists (select max(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ * from t1 where not exists (select max(a1) from t2) order by c1 ;

--echo  #like
select /*+no_rewrite*/ count(c1) from t1 where c1 like (select max(a1) from t2) order by c1 ;
select /*+parallel(6) no_rewrite*/ count(c1) from t1 where c1 like (select max(a1) from t2) order by c1 ;


--echo  #不同join
select /*+no_rewrite*/ count(*) from t1,t2 where t1.c1 > (select max(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ count(*) from t1,t2 where t1.c1 > (select max(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1,t2 where t1.c1 > (select max(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1,t2 where t1.c1 > (select max(a1) from t2) order by t1.c1;



select /*+no_rewrite*/ count(*) from t1 cross join t2 where t1.c1 in (select avg(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ count(*) from t1 cross join t2 where t1.c1 in (select avg(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1 cross join t2 where t1.c1 in (select avg(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1 cross join t2 where t1.c1 in (select avg(a1) from t2) order by t1.c1;


select /*+no_rewrite*/ count(*) from t1 natural join t2 where t1.c1 not in (select avg(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ count(*) from t1 natural join t2 where t1.c1 not in (select avg(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1 natural join t2 where t1.c1 not in (select avg(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1 natural join t2 where t1.c1 not in (select avg(a1) from t2) order by t1.c1;


select /*+no_rewrite*/ * from t1,t2 where t1.c1 = t2.a1 and t1.c1 = all (select count(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ * from t1,t2 where t1.c1 = t2.a1 and t1.c1 = all (select count(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ * from t1,t2 where t1.c1 = t2.a1 and t1.c1 = all (select count(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1,t2 where t1.c1 = t2.a1 and t1.c1 = all (select count(a1) from t2) order by t1.c1;


select /*+no_rewrite*/ * from t1 inner join t2 on t1.c1 = t2.a1 where t1.c1 < any (select min(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ * from t1 inner join t2 on t1.c1 = t2.a1 where t1.c1 < any (select min(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ * from t1 inner join t2 on t1.c1 = t2.a1 where t1.c1 < any (select min(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 inner join t2 on t1.c1 = t2.a1 where t1.c1 < any (select min(a1) from t2) order by t1.c1;

select /*+no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where t1.c1 > some (select min(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where t1.c1 > some (select min(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where t1.c1 > some (select min(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where t1.c1 > some (select min(a1) from t2) order by t1.c1;


select /*+no_rewrite*/ * from t1 right join t2 on t1.c1 = t2.a1 where exists (select max(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ * from t1 right join t2 on t1.c1 = t2.a1 where exists (select max(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ * from t1 right join t2 on t1.c1 = t2.a1 where exists (select max(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 right join t2 on t1.c1 = t2.a1 where exists (select max(a1) from t2) order by t1.c1;


select /*+no_rewrite*/ count(*) from t1,t2 where t1.c1 like (select max(a1) from t2) order by t1.c1;
explain basic select /*+no_rewrite*/ count(*) from t1,t2 where t1.c1 like (select max(a1) from t2) order by t1.c1;
select /*+parallel(6) no_rewrite*/ count(*) from t1,t2 where t1.c1 like (select max(a1) from t2) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ count(*) from t1,t2 where t1.c1 like (select max(a1) from t2) order by t1.c1;


--echo  #驱动相关子查询
select /*+no_rewrite*/ * from t1 where c1 < (select min(a1) from t2 where t2.a1 > t1.c1) order by c1;
explain basic select /*+no_rewrite*/ * from t1 where c1 < (select min(a1) from t2 where t2.a1 > t1.c1) order by c1;

select /*+no_rewrite*/ * from t1 where c1 <= (select min(a1) from t2 where t2.a1 <= t1.c1) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 <= (select min(a1) from t2 where t2.a1 <= t1.c1) order by c1;

select /*+no_rewrite*/ * from t1 where c1 > (select min(a1) from t2 where t2.a1 > t1.c1) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 > (select min(a1) from t2 where t2.a1 > t1.c1) order by c1;

select /*+no_rewrite*/ * from t1 where c1 >= (select avg(a1) from t2 where t2.a1 = t1.c1) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 >= (select avg(a1) from t2 where t2.a1 = t1.c1) order by c1;

select /*+no_rewrite*/ * from t1 where c1 <> (select min(a1) from t2 where t2.a1 <> t1.c1) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 <> (select min(a1) from t2 where t2.a1 <> t1.c1) order by c1;


select /*+no_rewrite*/ * from t1 where c1 in (select max(a1) from t2 where t2.a1 = t1.c1) order by c1;
explain basic select /*+no_rewrite*/ * from t1 where c1 in (select max(a1) from t2 where t2.a1 = t1.c1) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 in (select max(a1) from t2 where t2.a1 = t1.c1) order by c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 where c1 in (select max(a1) from t2 where t2.a1 = t1.c1) order by c1;

select /*+no_rewrite*/ * from t1 where c1 not in (select max(a1) from t2 where t2.a1 < t1.c1) order by c1;
explain basic select /*+no_rewrite*/ * from t1 where c1 not in (select max(a1) from t2 where t2.a1 < t1.c1) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 not in (select max(a1) from t2 where t2.a1 < t1.c1) order by c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 where c1 not in (select max(a1) from t2 where t2.a1 < t1.c1) order by c1;

select /*+no_rewrite*/ * from t1 where c1 < any (select min(a1) from t2 where t2.a1 > t1.c1) group by c1 having max(c1) > 4 order by c1;
explain basic select /*+no_rewrite*/ * from t1 where c1 < any (select min(a1) from t2 where t2.a1 > t1.c1) group by c1 having max(c1) > 4 order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 < any (select min(a1) from t2 where t2.a1 > t1.c1) group by c1 having max(c1) > 4 order by c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 where c1 < any (select min(a1) from t2 where t2.a1 > t1.c1) group by c1 having max(c1) > 4 order by c1;

select /*+no_rewrite*/ * from t1 where c1 > all (select min(a1) from t2 where t2.a1 <= t1.c1 group by a1 having min(a1) < 4 ) order by c1;
explain basic select /*+no_rewrite*/ * from t1 where c1 > all (select min(a1) from t2 where t2.a1 <= t1.c1 group by a1 having min(a1) < 4 ) order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 > all (select min(a1) from t2 where t2.a1 <= t1.c1 group by a1 having min(a1) < 4 ) order by c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 where c1 > all (select min(a1) from t2 where t2.a1 <= t1.c1 group by a1 having min(a1) < 4 ) order by c1;

select /*+no_rewrite*/ * from t1 where c1 <> some (select min(a1) from t2 where t2.a1 <> t1.c1 group by a1) group by c1 having min(c1) <3 order by c1;
explain basic select /*+no_rewrite*/ * from t1 where c1 <> some (select min(a1) from t2 where t2.a1 <> t1.c1 group by a1) group by c1 having min(c1) <3 order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where c1 <> some (select min(a1) from t2 where t2.a1 <> t1.c1 group by a1) group by c1 having min(c1) <3 order by c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 where c1 <> some (select min(a1) from t2 where t2.a1 <> t1.c1 group by a1) group by c1 having min(c1) <3 order by c1;

select /*+no_rewrite*/ * from t1 where  exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having min(c1) >3 order by c1;
explain basic select /*+no_rewrite*/ * from t1 where  exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having min(c1) >3 order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where  exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having min(c1) >3 order by c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 where  exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having min(c1) >3 order by c1;


select /*+no_rewrite*/ * from t1 where  not exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having max(c1) >5 order by c1;
explain basic select /*+no_rewrite*/ * from t1 where  not exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having max(c1) >5 order by c1;
select /*+parallel(6) no_rewrite*/ * from t1 where  not exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having max(c1) >5 order by c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 where  not exists (select a1 from t2 where t2.a1 = t1.c1) group by c1 having max(c1) >5 order by c1;


--echo  #不同join相关子查询
select /*+no_rewrite*/ count(*) from t1,t2 where c1 = all (select max(a1) from t2 where t1.c1 = t2.a1) order by t1.c1 ;
select /*+parallel(6) no_rewrite*/ count(*) from t1,t2 where c1 = all (select max(a1) from t2 where t1.c1 = t2.a1) order by t1.c1 ;

select /*+no_rewrite*/ max(c1) from t1 cross join t2 where c1 < any (select min(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ max(c1) from t1 cross join t2 where c1 < any (select min(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;

select /*+no_rewrite*/ min(c1) from t1 natural join t2 where c1 >= some (select avg(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
explain basic select /*+no_rewrite*/ min(c1) from t1 natural join t2 where c1 >= some (select avg(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ min(c1) from t1 natural join t2 where c1 >= some (select avg(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ min(c1) from t1 natural join t2 where c1 >= some (select avg(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;

select /*+no_rewrite*/ avg(c1) from t1,t2 where t1.c1 = t2.a1 and t1.c1 in (select max(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
explain basic select /*+no_rewrite*/ avg(c1) from t1,t2 where t1.c1 = t2.a1 and t1.c1 in (select max(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ avg(c1) from t1,t2 where t1.c1 = t2.a1 and t1.c1 in (select max(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ avg(c1) from t1,t2 where t1.c1 = t2.a1 and t1.c1 in (select max(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;

select /*+no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where exists (select min(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
explain basic select /*+no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where exists (select min(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
select /*+parallel(6) no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where exists (select min(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;
explain basic select /*+parallel(6) no_rewrite*/ * from t1 left join t2 on t1.c1 = t2.a1 where exists (select min(a1) from t2 where t1.c1 = t2.a1) order by t1.c1;



