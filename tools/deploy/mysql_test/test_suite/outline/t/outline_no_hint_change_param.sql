#####attention: 利用空格使得同样语义的sql命中不同的plan
#####如select c1 from t1;和select c1  from t1;将命中不同的plan

############just clear the data
delete            from t1;
delete            from t2;
delete            from t3;
delete            from ts1;
delete            from ts2;
delete            from tu1;
delete            from tu2;
delete            from td1;
delete            from tdd1;
delete            from td2;
delete            from td3;
delete            from td4;
############### test insert
insert into ts1 values(4,2),(5,2),(6,3);
select * from ts1;
insert into ts2 values(24,2),(25,2),(26,3);
select * from ts2;
delete       from ts2;
insert into ts1 select * from ts2;
select *   from ts1;
delete from ts1;
insert into ts1 select c1,c2 from ts1 union select c1,c2 from ts2;
select *    from ts1;

insert into ts1 values (212,  null);
select  * from ts1;
insert into ts1 values(213, null+null);
select   * from ts1;
insert into   ts1 values(241, 1+null);
select    * from ts1;
##expect 0, because 1+null and null + 1 need mache type
insert into    ts1 values(31, null+2);
select     * from ts1;

############### test update
insert into tu1 values (4,3,'yyy'), (5,2,'hhhh'), (6,1,'lalala');
insert into tu2 values (4,3,'yyy'), (5,2,'hhhh'), (6,1,'lalala');
update tu1 set c2 = 2 where c1 = 2;
select * from tu1;
update tu1 t set t.c2 = 2 where c1 = 1;
select *  from tu1;
update tu1 set c2 = c2 + 101, c3 = 'test' where c1 = 1;
select *   from tu1;
update tu1 set c2 = c3 like '%h%' where c1 = 2;
select *    from tu1;
update tu1 set c2 = 11 where c2 = 1 and c3 = 'yyy';
select *     from tu1;

update tu1 set c2=c2+2 order by c1;
select  * from tu1;
update tu1 set c2=999 where c1=2 order by c2 asc limit 1;
select   * from t1 order by c1,c2;
update tu1 set c2=101 where c1=3 order by c2 desc limit 2;
select    * from tu1;
update tu1 set c1=c1+11+c2 where c1=1 order by c2;
select     * from tu1;
update tu1 set c2 = (select distinct 2 from (select * from tu2) a);
select      * from tu1;

#test delete
delete from td1 where c1 in (select max(c1) from t1 t2);
delete from td1 where c1 in (select min(c2) from (select max(c1) as c2 from td1 ) t2);
replace into td1 values (2,2),(3,4);
update td1 set c1 = 6 where c1 in (select max(c1) from td1);
replace into td1 values(1,1),(2,3),(3,3),(4,3);

delete from td1 where td1.c1 <> (select c2 from tdd1 where td1.c1 < tdd1.c1 limit 2);
DELETE FROM td1 WHERE td1.c1 > 11 ORDER BY td1.c1;
DELETE FROM td1 WHERE td1.c1 > 10 ORDER BY td1.c1 LIMIT 1;
DELETE FROM td1 ORDER BY c1 ASC, c2 ASC LIMIT 2;
DELETE FROM td1 WHERE c1 = 10 OR c2 = 21 ORDER BY c1 LIMIT 1;

#single column rowkey
insert into td2 values(4,0),(5,0),(6,1);
delete from td2;
select * from td2;

replace into td2  values(1,0),(2,0),(4,0);
delete from td2  where c1>2;
select * from  td2;

replace   into td2 values(12,0),(2,0),(3,0);
delete  from td2 where c1=2;
select *   from td2;

replace    into td2 values(12,0),(2,0),(3,0);
delete   from td2 where c1=2 or c1=2 or c1=3;
select *    from td2;

replace     into td2 values(12,1),(2,2),(3,3);
delete from td2 where c1<=12;
select *     from td2;
delete from td2 where 2>=c1;
select *       from td2;

replace      into td2 values(21,1),(2,2),(3,3);
delete from td2 where c1=1 or c1=3;
select *        from td2;
delete from td2 where c1=1 and c1=3;
select *         from td2;

replace into td2(c1,c2) values(11,1),(2,1),(3,1),(4,1);
delete from td2 where c1>=2 and c1<4;
select * from   td2;

replace  into td2(c1,c2) values(11,1),(2,2),(3,1),(4,2);
delete from td2 where c1>0 and c2>11;
delete from td2 where c1<5 and c2<21;
select * from    td2;

replace   into td2(c1,c2) values(1,11),(2,2),(3,1),(4,2);
delete from td2 where c1=0 or c1=11 or c1=2 or c2=1;
select *  from    td2;

replace into td2(c1,c2) values(11,NULL),(1,1),(2,1),(3,1),(4,1);
#rowkey
delete from td2 where c1 in (11,2,3,4);
select c1,c2 from td2;

replace  into td2(c1,c2) values(12,NULL),(1,1),(2,1),(3,1),(4,1);
delete from td2 where c1 in (NULL,2) or c1 in (2,3,4);
select c1,c2  from td2;

replace   into td2(c1,c2) values(122,NULL),(1,1),(2,1),(3,1),(4,1);
delete from td2 where c1 in (2) or c1 in (2) or c1 in (3,4);
select c1,c2   from td2;

replace    into td2(c1,c2) values(133,NULL),(1,1),(2,1),(3,1),(4,1);
delete from td2 where c1 in (NULL,1,2) and c1 in (1,2,3,4);
select c1,c2    from td2;

replace     into td2(c1,c2) values(15,NULL),(1,1),(2,1),(3,1),(4,1);
delete from td2 where c1 in (1,2,3,41) and c2 in (1,2,3,4);
select c1,c2     from td2;

replace into td3 values(1,1,1,0),(21,2,2,0),(3,3,3,0),(4,4,4,0),(5,4,4,0),(6,4,4,0),(7,4,6,0),(8,4,6,0),(9,4,6,0),(10,4,6,0),(11,4,6,0),(12,4,6,0), (13,4,6,0);
delete from td3 where p1 = 1;
select * from td3;
replace into   td3 values(1,1,1,0),(2,2,2,0),(3,3,3,0),(4,4,4,0),(5,4,4,0),(6,4,4,0),(7,4,6,0),(8,4,6,0),(9,4,6,0),(10,4,6,0),(11,4,6,0),(12,4,6,0), (13,4,6,0);
delete from td3 where p1>3 or p3 >=61;
select *  from td3;
delete from td3 where p1=1 and p3 =3;
select *   from td3;
delete from td3 where p1=1 and p2 =2 and p3=1;
select *    from td3;
replace into td3   values(1,1,1,0),(21,2,2,0),(3,3,3,0),(4,4,4,0),(5,4,4,0),(6,4,4,0),(7,4,6,0),(8,4,6,0),(9,4,6,0),(10,4,6,0),(11,4,6,0),(12,4,6,0), (13,4,6,0);

delete from td3 where p1 in (11,2,3,6,7,8,12,13,0);
select *     from td3;

replace   into td3   values(11,1,1,0),(2,2,2,0),(3,3,3,0),(4,4,4,0),(5,4,4,0),(6,4,4,0),(7,4,6,0),(8,4,6,0),(9,4,6,0),(10,4,6,0),(11,4,6,0),(12,4,6,0), (13,4,6,0);
delete from td3   where p1 in (13,2,3,6,7,8,12,13,0);
select *      from td3;

replace into td4(a,b,c,d) values(11,'1','2014-02-17',NULL),(2,'2','2014-02-17',NULL),(3,'3','2014-02-17',NULL);
delete from td4 where (a,b,c) in ((11,'1','2014-02-17'));
delete from td4 where (a,b,c) in ((21,'2','2014-02-17'),(3,'3','2014-02-17'));
select * from td4;

replace into  td4(a,b,c,d) values(11,'1','2014-02-17',NULL),(2,'2','2014-02-17',NULL),(3,'3','2014-02-17',NULL);
delete from td4 where (a,b,c) in ((11,'1','2014-02-17'),(2,'2','2014-02-17'),(3,'3','2014-02-17'));
select *  from td4;

replace into   td4(a,b,c,d) values(11,'1','2014-02-17',NULL),(2,'2','2014-02-17',NULL),(3,'3','2014-02-17',NULL);
delete from td4 where (a,b,c) in ((NULL,NULL,NULL),(11,'1','2014-02-17'),(2,'2','2014-02-17'),(4,'4','2014-02-17'));
select *   from td4;

replace into    td4(a,b,c,d) values(11,'1','2014-02-17',NULL),(2,'2','2014-02-17',2),(3,'3','2014-02-17',2);
delete from td4 where (a,b,c,d) in ((11,'1','2014-02-17',NULL),(2,'2','2014-02-17',2),(3,'3','2014-02-17',2));
select *    from td4;

replace into     td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where (a,b,c) in ((11,'1','2014-02-17'),(2,'2','2014-02-17'),(3,'3','2014-02-17')) and d in (2,2);
select *     from td4;
delete from  td4 where (a,b,c) in ((11,'1','2014-02-17'),(2,'2','2014-02-17'),(3,'3','2014-02-17')) and d in (1,2);
select *      from td4;

replace into      td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where (a,b,c) in ((11,'1','2014-02-17')) or (a,b,c) in ((2,'2','2014-02-17'),(3,'3','2014-02-17')) and d in (1);
select *        from td4;

replace into       td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where (a,b,c) in ((11,'1','2014-02-17'),(2,'2','2014-02-17')) and (a,b,c) in ((2,'2','2014-02-17'),(3,'3','2014-02-17')) and d>0;
select * from  td4;

#non_rowkey
replace  into td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where (a) in ((11),(2));
select * from   td4;
delete from td4 where (a,b) in ((31,'3'),(2,'2'));
select * from    td4;

replace   into td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where (b,c,a) in (('11','2014-02-17',1),('2','2014-02-17',2));
select * from     td4;

replace    into td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where (a,b,c) in ((11,'1','2014-02-17'),(2,'2','2014-02-17')) or (b,c) in (('2','2014-02-17'),('3','2014-02-17'));
select * from       td4;

replace     into td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where c='2014-02-17';
select *  from  td4;

replace      into td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where a=31;
select *  from   td4;

replace       into td4(a,b,c,d) values(11,'1','2014-02-17',1),(2,'2','2014-02-17',1),(3,'3','2014-02-17',1);
delete from td4 where a=1 and b = '2' and c = '2014-02-17' and d=1;
select *  from    td4;
delete from td4 where a in (select a from td4);
select *  from     td4;

############ test select
delete from     t1;
delete from     t2;
delete from     t3;
insert into t1 value(44, 2, 4), (5,2,3), (6,2,2), (7,2,1);
insert into t1 value(111, 3, 4), (112,3,3), (113,3,2), (114,3,1), (115,4,1);
insert into t2 value(121, 4, 4), (122,4,3), (123,4,2), (124,4,1);
insert into t3 value(121, 4, 4), (122,4,3), (123,4,2), (124,4,1);
###select from dual
SELECT 12 + 46;
SELECT 12 + 46 FROM DUAL;
SELECT 12 + 45 FROM DUAL WHERE 10 > 6;
SELECT 12 + 45 FROM DUAL WHERE 10 > 6 LIMIT 10;
SELECT 12 + 45 FROM DUAL LIMIT 1;
SELECT 12 + 45 LIMIT 11;

#select having and group by
--sorted_result
select * from t1 group by c2 order by c2 limit 2, 2;
--sorted_result
select c1, c2 as c1 from t1 group by t1.c1;
--sorted_result
select c1 as c1, c2 as c1 from t1 group by t1.c1;
--sorted_result
select c1, c1 from t1 group by c1;
--sorted_result
select 1 as c1, 2 as c2 from t1 group by c1 limit 1;
--sorted_result
select c1 as c2, c2 as c1 from t1 group by c1;
select c1, c2 as c1 from t1 having t1.c1 > 3;
select c1, c2 + 1 as c1 from t1 having t1.c1 > 4;
select * from t1 left join t2 on t1.c1=t2.c1;
select * from t1 left join t2 on t1.c1=t2.c1 having not (t2.c1 <=> t1.c1);
--sorted_result
select c1+1 AS c1 FROM t1 GROUP BY c1;
--sorted_result
select c2 as c1 from t1 group by c1;
--sorted_result
select c1+1 as c1 from t1 group by c1+1;

#select like
select 'b' = 'b ', 'a' like 'a ';
select 'davidy' like 'david\_';
select 'davidy' like 'david|_' escape '|';
select 'abcd' like 'abcd';
select 'abcd' like binary 'abcd';
select 11 like '1%';
select 'aavid_' like 'aavid|_' escape null;

#select use or force key
select * from t1 use key(idx);
select * from t1 force index(idx);
select * from t1 ignore key(idx);
select * from t1 use key(a);
select * from t1 force index(a);
select * from t1 ignore key(a);

#select join and union
select * from t1;
select * from (t1);
select * from (t1,t2);
select * from (t1,(t2));
select * from (t3,((t1,t2)));
select * from t1 join t2 join t3;
select * from (t1 join t2) join t3;
select * from (t1 join t2) join (select * from t3) a;
select * from ((t1 join t2) join (select * from t3) a);
select * from t1 join t2 on (t1.c1=t2.c1) join t3 on (t1.c1=t2.c1);
select * from t1 join t2 using (c1) join t3 using (c1);
select * from t1 join t2 on (t1.c1=t2.c1);
(select count(*) from t1) UNION (select count(*) from t1);
(select c1,c2 from t1 limit 3)  union all (select c1,c2 from t2 order by c1 limit 1) order by c1;
(select * from t1 order by c1 limit 2) union (select * from t2 order by c1 limit 3) limit 2;
(select * from t1 order by c1 limit 2) union (select * from t2 order by c1 limit 3);
(select * from t1 order by c1 limit 2) union all (select * from t2 order by c1 limit 3);
(select * from t1 order by c1 limit 2) union distinct (select * from t2 order by c1 limit 3);
select * from t1,t2,t3 where t1.c1=t2.c1 AND t2.c2=t3.c1 and t1.c2=t3.c2 order by t1.c1;

#select view
select a.c1 from (select c1 from t1) as a having a.c1 = 1;

#select with where condition
select * from t1 where 'c' not like 'b' and ((c1 < 2 and false) or (c1 < 2 and c1 is not null)) ;
select * from t1 where 'c' not like 'b' and ((false) or (c1 < 2 and c1 is not null));

#select subquery
select * from t1 group by 3>(select count(*) from t2);
select * from t1 order by 3>(select count(*) from t2);
select (3>(select count(*) from t2)) from t1;
select * from t1 where 3>(select count(*) from t2);
select (3>(select count(*) from t2)) from t1 union select c1 from t2;
select * from t1 group by 3>(select count(*) from t2) union select * from t2;
(select * from t1 order by 3>(select count(*) from t2)) intersect select * from t2;
select * from t1 where 3>(select count(*) from t2) union select * from t2;

#select aggr
select min(2) from t1;
select max(2) from t1;
select sum(2) from t1;
select avg(2) from t1;
select max(t3.c1) from t1 as t3, t1 t4 where t3.c1 = t4.c1;

#select function
SELECT length('asdfgyyy');
select found_rows();

##select for update
select * from  t1  for update wait 2;
select * from  t1  a  for update wait 2;
select * from  t1  where c1 <101 for update wait 1;
select * from  t1  where c1 <101  and c2 < 100 for update wait 1;
select * from  t1  where c1 in (3,2,3,4,5,6) for update wait 1;
select * from  t1  where (c1,c2) in ((2,2), (2,3),(3,4)) for update wait 1;
select * from t1 t where t.c2 = 3 and t.c1 = 2 for update nowait;
select abs(c1) , sum(c2) from t1 group by abs(c2) having sum(sign(c1)) > 1 for update;
