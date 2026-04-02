--echo ##################################DML number_format_model-1-9##################################

--disable_warnings
--error 0,942
drop table format_test1;
--enable_warnings
create table format_test1(c1 number,c2 binary_float,c3 binary_double,c4 char(100),c5 nchar(100),c6 varchar2(100),c7 nvarchar2(100),c8 blob,c9 clob,c10 date);
insert into format_test1 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(100,0.00,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(100,1234.34,1234,'100.00','23.12','100.2356','1234','12','1234',to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(100,100.2356,1234,'100.00','23.12','100.2356','1234','123','12345',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));
insert into format_test1 values(100,200,200,'200','200','200','200','200','200',to_date('2021-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss'));

####单行、多行、关联子查询

select max(to_number(t1.c1,'999')) from format_test1 t1;
select max(to_number(t2.c1,'999')) from format_test1 t2 where to_number(t2.c1,'999') = (select max(to_number(t1.c1,'999')) from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_test1 t2 where to_number(t2.c1,'999') in (select max(to_number(t1.c1,'999')) from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_test1 t2 where to_number(t2.c1,'999') in (select to_number(t1.c1,'999') from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_test1 t2 where to_number(t2.c1,'999') >= any(select max(to_number(t1.c1,'999')) from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_test1 t2 where to_number(t2.c1,'999') not in (select to_number(t1.c1,'999') from format_test1 t1);

####rownum,rowid
select max(to_number(t1.c1,'999')) from format_test1 t1 where rownum =1;
--error 01722
select max(to_number(t1.c1,'999')) from format_test1 t1 where to_number(rownum,'9,$99999999B') =1;
select max(to_number(t1.c1,'999')) from format_test1 t1 where to_number(rownum,'999') =1;
--error 01722
select max(to_number(t1.c1,'999')) from format_test1 t1 where to_number(rownum,'000') =1;
--error 01722
select max(to_number(t1.c1,'999')) from format_test1 t1 where to_number(rownum,'fmtme') =1;
--error 01722
select max(to_number(t1.c1,'999')) from format_test1 t1 where to_number(rownum,'fmtm9') =1;
select max(to_number(t1.c1,'999')) from format_test1 t1 where to_number(rownum,'FM9B') =1;
select max(to_number(t1.c1,'999')) from format_test1 t1 where to_number(rownum,'FM99PR') =1;


###不同类型之间相互比较
select max(to_number(c1,'999')) from format_test1 where c1 = c2;
select max(to_number(c1,'999')) from format_test1 where c1 = c3;
select max(to_number(c1,'999')) from format_test1 where c1 = c4;
select max(to_number(c1,'999')) from format_test1 where c1 = c5;
select max(to_number(c1,'999')) from format_test1 where c1 = c6;


--disable_warnings
--error 0,942
drop view format_testview6;
--enable_warnings
create view format_testview6 as select * from format_test1;

select max(to_number(c1,'999')) from format_testview6 where c1 = c2;
select max(to_number(c1,'999')) from format_testview6 where c1 = c3;
select max(to_number(c1,'999')) from format_testview6 where c1 = c4;
select max(to_number(c1,'999')) from format_testview6 where c1 = c5;
select max(to_number(c1,'999')) from format_testview6 where c1 = c6;
select max(to_number(t1.c1,'999')) from format_testview6 t1;

select max(to_number(t2.c1,'999')) from format_testview6 t2 where to_number(t2.c1,'999') = (select max(to_number(t1.c1,'999')) from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_testview6 t2 where to_number(t2.c1,'999') in (select max(to_number(t1.c1,'999')) from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_testview6 t2 where to_number(t2.c1,'999') in (select to_number(t1.c1,'999') from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_testview6 t2 where to_number(t2.c1,'999') >= any(select max(to_number(t1.c1,'999')) from format_test1 t1);
select max(to_number(t2.c1,'999')) from format_testview6 t2 where to_number(t2.c1,'999') not in (select to_number(t1.c1,'999') from format_test1 t1);

####DML验证number_format_model-1-9-2/number_format_model-1-9-3/number_format_model-1-9-4

insert into format_test1 select * from format_test1;
insert into format_test1 select * from format_testview6;

--error 00939
update format_test1 t set t.c2 = 300 where to_number(t.c2,'FM99PR') = to_number(t.c1,'FM99PR') and rownum =1;
--error 00939
update format_test1 t set t.c2 = (select e.c2 from format_test1 e where e.c1 = 200) where to_number(t.c2,'FM99PR') = to_number(t.c1,'FM99PR') and rownum =1;
update format_test1 t set t.c2 = 300 where t.c2 in (select e.c2 from format_test1 e where e.c1 = 200);
update (select 
t2.c1 c1old,t2.c2 c2new from format_testview6 t2 where to_number(t2.c1,'999') = (select max(to_number(t1.c1,'999')) from format_test1 t1)) set c1old = c2new;
--error 01722
update format_test1 t set (t.c1,t.c2) = (select 
t2.c1 c1old,t2.c2 c2new from format_testview6 t2 where to_number(t2.c1,'999') = (select max(to_number(t1.c1,'999')) from format_test1 t1) and rownum =1);

--error 00939
delete from (select * from format_test1 t where to_number(t.c2,'FM99PR') = to_number(t.c1,'FM99PR'));
delete from format_test1 t where t.c1 in (select e.c2 from format_testview6 e);


--disable_warnings
--error 0,942
drop table format_test1;
--error 0,942
drop table format_test2;
--error 0,942
drop table format_test3;
--error 0,942
drop table format_test4;
--error 0,942
drop table format_test1;
--error 0,942
drop table format_test1;
--error 0,942
drop view format_testview6;
--enable_warnings
