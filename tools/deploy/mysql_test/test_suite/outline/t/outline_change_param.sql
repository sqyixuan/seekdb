##select
select  c2,c1 from t1 where c2 = 3;
select  c2,c1 from t1 where c2 = 2 order by 2;
select * from (select c2,c1 from t1 where c1 >2) as tt;
select c2,c1 from t1 where c1 < any(select c1 from t2 where c1 >2);
