## test simulate error -4038 in inner table select
select 1 as a from (select count(1) from oceanbase.__all_dummy) t;
select 1 as a from (select count(1) from oceanbase.__all_table) t;
select 1 as a from (select count(1) from oceanbase.V$OB_MEMORY) t;

## test simulate error -4038 in dml stmt
insert into t1 values(1000, 1000);
select * from t1 where a=1000;
update t1 set b=2000 where a=1000;
select * from t1 where a=1000;
delete from t1 where a=1000;
select * from t1 where a=1000;

# join, check value
select * from t1;
select * from pt2;
select * from t1 join pt2 on t1.a = pt2.a where t1.a < 100;
select * from pt1;
select * from pt1 join pt2 on pt1.a = pt2.a where pt1.a < 100;
select * from pt1 join pt2 on pt1.a = pt2.a where pt1.a < pt2.a;
select * from pt3;
select * from pt1 join pt3 on pt1.a = pt3.a where pt1.a < 100;
select * from pt3 join pt2 on pt3.a = pt2.a where pt2.a < 100 and pt3.a < 100;
select * from pt1 join pt2 on pt1.a = pt2.a where pt2.a = pt1.a and pt1.a >= pt2.a;
select * from pt1 join pt3 on pt1.a = pt3.a where pt1.a = 1;
select * from pt1,pt2 where pt1.a=pt2.a;
select * from pt1,pt2 where pt1.a>pt2.a;
select * from pt1,pt2 where pt1.a=pt2.a and pt1.a >= pt2.a;
select * from pt1,pt2,pt3 where pt1.a = pt2.a and pt1.a = pt3.a;
select * from pt4;
select * from pt1,pt4 where pt1.a=pt4.a;
select * from pt6;
select * from pt4,pt6 where pt4.a = pt6.a;
select * from pt5;
select * from pt4,pt5,pt6 where pt4.a=pt5.a and pt4.a=pt6.a;
select * from pt7;
select * from pt5,pt7 where pt5.a=pt7.a and pt5.b=pt7.b;
select * from pt8;
select * from pt7,pt8 where pt7.a=pt8.a and pt7.b=pt8.b;

select * from pkey1;
select * from pt1,pkey1,pkey2 where pt1.a=pkey1.a and pt1.a = pkey2.a;
select * from pkey2;
select * from pkey1,pkey2 where pkey1.a=pkey2.a;
select * from pkey3;
select * from pkey1,pkey2,pkey3 where pkey1.a=pkey2.a and pkey1.a=pkey3.a;
select * from pkey4;
select * from pkey3,pkey4 where pkey3.a = pkey4.a;
select * from pkey3,pkey4 where pkey3.a=pkey4.a and pkey3.b=pkey4.b;
select * from pkey1,pkey2,pkey3,pkey4 where pkey1.a=pkey2.a and pkey3.a=pkey4.a and pkey3.b=pkey4.b;
select * from pkey1,pkey3,pkey4 where pkey1.a = pkey3.a and pkey3.a=pkey4.a and pkey3.b=pkey4.b;

select * from pr1,pr2 where pr1.a=pr2.a;

select * from prc1,prc2 where prc1.a=prc2.a;
select * from prc1,prc2 where prc1.a=prc2.a and prc1.a < '2015-01-01';
select * from prc1,prc2,prc3 where prc1.a=prc2.a and prc1.a=prc3.a;
select * from prc2,prc3 where prc2.a=prc3.a;

select * from sp1,sp2 where sp1.a=sp2.a;

#hash
select * from t1 join pt2 on t1.a = pt2.a where t1.a < 100;
select * from pt1 join pt2 on pt1.a = pt2.a where pt1.a < 100;
select * from pt1 join pt2 on pt1.a = pt2.a where pt1.a < pt2.a;
select * from pt1 join pt3 on pt1.a = pt3.a where pt1.a < 100;
select * from pt3 join pt2 on pt3.a = pt2.a where pt2.a < 100 and pt3.a < 100;
select * from pt1 join pt2 on pt1.a = pt2.a where pt2.a = pt1.a and pt1.a >= pt2.a;
select * from pt1 join pt3 on pt1.a = pt3.a where pt1.a = 1;
select * from pt1,pt2 where pt1.a=pt2.a;
select * from pt1,pt2 where pt1.a>pt2.a;
select * from pt1,pt2 where pt1.a=pt2.a and pt1.a >= pt2.a;
select * from pt1,pt2,pt3 where pt1.a = pt2.a and pt1.a = pt3.a;
select * from pt1,pt4 where pt1.a=pt4.a;
select * from pt1, pt2, pt9 where pt1.a = pt2.a and pt2.a = pt9.a;
select * from pt1, pt2, pt9 where pt1.a = pt2.a and pt1.a = pt9.a;
select * from pt1, pt2, pt9 where pt1.a = pt9.a and pt2.a = pt9.a;
select * from pt4,pt6 where pt4.a = pt6.a;
select * from pt4,pt5,pt6 where pt4.a=pt5.a and pt4.a=pt6.a;
select * from pt5,pt7 where pt5.a=pt7.a and pt5.b=pt7.b;
select * from pt7,pt8 where pt7.a=pt8.a and pt7.b=pt8.b;

#key
select * from pt1,pkey1,pkey2 where pt1.a=pkey1.a and pt1.a = pkey2.a;
select * from pkey1,pkey2 where pkey1.a=pkey2.a;
select * from pkey1,pkey2,pkey3 where pkey1.a=pkey2.a and pkey1.a=pkey3.a;
select * from pkey3,pkey4 where pkey3.a = pkey4.a;
select * from pkey3,pkey4 where pkey3.a=pkey4.a and pkey3.b=pkey4.b;
select * from pkey1,pkey2,pkey3,pkey4 where pkey1.a=pkey2.a and pkey3.a=pkey4.a and pkey3.b=pkey4.b;
select * from pkey1,pkey3,pkey4 where pkey1.a = pkey3.a and pkey3.a=pkey4.a and pkey3.b=pkey4.b;

#range 
select * from pr1,pr2 where pr1.a=pr2.a;
select * from pr2, pr3 where pr2.a = pr3.a;
select * from pr1, pr2, pr3 where pr1.a = pr2.a and pr2.a = pr3.a;
select * from pr1, pr2, pr4 where pr1.a = pr2.a and pr2.a = pr4.a;

# range column wise-join
select * from prc1,prc2 where prc1.a=prc2.a;
select * from prc1,prc2 where prc1.a=prc2.a and prc1.a < '2015-01-01';
select * from prc1,prc2,prc3 where prc1.a=prc2.a and prc1.a=prc3.a;
select * from prc2,prc3 where prc2.a=prc3.a;
select * from prc2,prc3,prc4 where prc2.a = prc3.a and prc3.a = prc4.a;

#hash,key
select * from pt1,pt2,pkey1,pkey2 where pt1.a=pt2.a and pkey1.a=pkey2.a;
select * from pt1,pkey1 where pt1.a=pkey1.a;
select * from pt1,pt2,pkey1 where pt1.a=pt2.a and pkey1.a < 100;
select * from pt1,pt2,pkey1 where pt1.a=pt2.a and pkey1.a=pt1.a and pkey1.a < 100;
select * from pt1,pt2,pkey1 where pt1.a<100 or pt2.a<100 or pkey1.a<100;
select * from pt1,pt2,pkey1 where pt1.a<100 and pt2.a<100 and pkey1.a<100;

#subpartition
select * from sp1,sp2 where sp1.a=sp2.a;
select * from sp1,sp2 where sp1.b=sp2.b;
select * from sp1,sp2 where sp1.a=sp2.a and sp1.b=sp2.b;
select * from sp1, sp2, sp3 where sp1.a = sp2.a and sp1.b = sp2.b and sp1.a = sp3.a and sp2.b = sp3.b;

select * from t2, t3 where t2.a = t3.a;
insert into t2 select * from t3;

select * from t_r4_int t1, t_r4_date as t2 where t1.a = t2.c;
