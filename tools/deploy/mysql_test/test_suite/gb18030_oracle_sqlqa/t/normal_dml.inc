
--disable_warnings
--error 0,1418
DROP index name_index1;
--error 0,942
drop table mail3 CASCADE CONSTRAINTS purge;
--enable_warnings

create table mail3
(
t number(6,2),
srcuser nvarchar2(200),
srchost varchar(200) default 'hahaha',
dstuser blob,
dsthost clob not null,
sizesize number(8,2)
);

create index name_index1 on mail3(srcuser,srchost);

insert into mail3 values(1,'中文测试1','中文测试1','aaabbb','中文测试1',100);
insert into mail3 values(1,'中文测试2','中文测试1','cccddd','中文测试1',100);

select count(*) from mail3 t where t.srcuser = t.srchost;
--error 932
select count(*) from mail3 t where t.srcuser = t.dstuser;
--error 0,932
select count(*) from mail3 t where t.srcuser = t.dsthost;
--error 0,932
select count(*) from mail3 t where t.srchost = t.dstuser;
--error 0,932
select count(*) from mail3 t where t.srchost = t.dsthost;
--error 0,932
select count(*) from mail3 t where t.dstuser = t.dsthost;


####pk where条件中比较字段查看，判断内容+判断方法
--disable_warnings
--error 0,1418
DROP index name_index1;
--error 0,942
drop table mail CASCADE CONSTRAINTS purge;
--enable_warnings

create table mail
(
t int,
srcuser nvarchar2(100) default 'aaaaa001',
srchost char(200) NOT NULL,
dstuser varchar2(200),
dsthost nchar(200),
tablesize number(10,2)
);

create index name_index1 on mail(srcuser,srchost);

insert into mail values(1,'中文测试1','中文测试2','中文测试3','中文测试4',100);
insert into mail values(3,01245,'中文测试9','中文测试10','中文测试11',300);
insert into mail values(5,'哈哈哈哈hhhh','中文测试12','中文测试13','中文测试14',500);
insert into mail values(6,'hhhh465456\\','中文测试12','中文测试13','中文测试14',600);
insert into mail values(7,'abc def ghi','中文测试12','中文测试13','中文测试14',700);
insert into mail values(9,'бжb','中文测试12','中文测试13','中文测试14',900);
insert into mail values(12,default,'中文测试12','中文测试13','中文测试14',1100);
insert into mail values(10,'??b','中文测试12','中文测试13','中文测试14',1000);
insert into mail values(11,'μ?周?б','中文测试12','中文测试13','中文测试14',1100);
insert into mail values(8,'????b','中文测试12','中文测试13','中文测试14',800);

###--echo  ##判断内容中文+null+空值+其他，方法相等，不等及其他包含关系
--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-6 
--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-1
select t.srcuser as 中文测试ing from mail t where t.srcuser ='中文测试1';
select t.srcuser as 中文测试ing from mail t where t.srcuser !='中文测试1';
select t.srcuser as 中文测试ing from mail t where t.srcuser <>'中文测试1';
select t.srcuser as 中文测试ing from mail t where t.srcuser <'中文测试1';
select t.srcuser as 中文测试ing from mail t where t.srcuser >='中文测试1';

select t.srcuser as 中文测试ing from mail t where t.srcuser in ('中文测试1');
select t.srcuser as 中文测试ing from mail t where t.srcuser not in ('中文测试1');
select t.srcuser as 中文测试ing from mail t where t.srcuser >='中文测试1' and t.srcuser <='中文测试1';
select t.srcuser as 中文测试ing from mail t where t.srcuser >='中文测试1' or t.srcuser <='中文测试1';
select t.srcuser as 中文测试ing from mail t where t.srcuser like'中文测试%';

select t.srcuser as 中文测试ing from mail t where t.srcuser =NULL;
select t.srcuser as 中文测试ing from mail t where t.srcuser !=NULL;
select t.srcuser as 中文测试ing from mail t where t.srcuser <>NULL;
select t.srcuser as 中文测试ing from mail t where t.srcuser <NULL;
select t.srcuser as 中文测试ing from mail t where t.srcuser >=NULL;

select t.srcuser as 中文测试ing from mail t where t.srcuser in (NULL);
select t.srcuser as 中文测试ing from mail t where t.srcuser not in (NULL);
select t.srcuser as 中文测试ing from mail t where t.srcuser >=NULL and t.srcuser <=NULL;
select t.srcuser as 中文测试ing from mail t where t.srcuser >=NULL or t.srcuser <=NULL;
select t.srcuser as 中文测试ing from mail t where t.srcuser like 'NULL%';

select t.srcuser as 中文测试ing from mail t where t.srcuser ='';
select t.srcuser as 中文测试ing from mail t where t.srcuser !='';
select t.srcuser as 中文测试ing from mail t where t.srcuser <>'';
select t.srcuser as 中文测试ing from mail t where t.srcuser <'';
select t.srcuser as 中文测试ing from mail t where t.srcuser >='';

select t.srcuser as 中文测试ing from mail t where t.srcuser in ('');
select t.srcuser as 中文测试ing from mail t where t.srcuser not in ('');
select t.srcuser as 中文测试ing from mail t where t.srcuser >='' and t.srcuser <='';
select t.srcuser as 中文测试ing from mail t where t.srcuser >='' or t.srcuser <='';
select t.srcuser as 中文测试ing from mail t where t.srcuser like'%';

select t.srcuser as 中文测试ing from mail t where t.srcuser ='01245';
select t.srcuser as 中文测试ing from mail t where t.srcuser !='01245';
select t.srcuser as 中文测试ing from mail t where t.srcuser <>'01245';
select t.srcuser as 中文测试ing from mail t where t.srcuser <'01245';
select t.srcuser as 中文测试ing from mail t where t.srcuser >='01245';

select t.srcuser as 中文测试ing from mail t where t.srcuser in ('01245');
select t.srcuser as 中文测试ing from mail t where t.srcuser not in ('01245');
select t.srcuser as 中文测试ing from mail t where t.srcuser >='01245' and t.srcuser <='01245';
select t.srcuser as 中文测试ing from mail t where t.srcuser >='01245' or t.srcuser <='01245';
select t.srcuser as 中文测试ing from mail t where t.srcuser like'01245%';

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-8 
###--echo  ##group by having条件查看,对应测分
SELECT srcuser, COUNT(*) FROM  mail GROUP BY srcuser having COUNT(*) >1;

--disable_warnings
--error 0,1418
DROP index name_index;
--error 0,942
drop table mail2 CASCADE CONSTRAINTS purge;
--enable_warnings

create table mail2
(
t2 int,
srcuser2 nvarchar2(100),
srchost2 char(200),
dstuser2 varchar2(200),
dsthost2 nchar(200),
size2 int,
constraint name_unique unique(srcuser2,dstuser2)
);

create index name_index on mail2(srcuser2);

insert into mail2 values(1,'中文测试1','中文测试2','中文测试3','中文测试4',100);
insert into mail2 values(3,01245,'中文测试9','中文测试10','中文测试11',300);
insert into mail2 values(4,'','中文测试12','中文测试13','中文测试14',400);
insert into mail2 values(5,'哈哈哈哈','中文测试12','中文测试13','中文测试14',500);
insert into mail2 values(6,'hahaha','中文测试12','中文测试13','中文测试14',600);

###--echo  ##join各种连接，对应测分

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-5 
--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-14
SELECT count(*) from mail e INNER JOIN mail2 d on e.srcuser = d.srcuser2;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2;
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srcuser2 is NULL;
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 union
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srcuser2 is NULL union
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL;

SELECT count(*) from mail e INNER JOIN mail2 d on e.srcuser = d.srcuser2 where e.srcuser = d.srcuser2 and e.srcuser ='中文测试1' ;
SELECT * from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 where e.srcuser = d.srcuser2 and e.srcuser ='中文测试1' ;
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 where e.srcuser = d.srcuser2 and e.srcuser ='中文测试1' ;
SELECT * from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srcuser2 is NULL;
SELECT e.srcuser from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srcuser2 is NULL;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srcuser2 is NULL and e.srcuser like '??%';
SELECT * from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL;
SELECT * from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL and d.srcuser2 like '中文测%' ;
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 union
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 where e.srcuser = d.srcuser2 and e.srcuser ='中文测试1' ;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srcuser2 is NULL union
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL and e.srcuser like '??%';

SELECT count(*) from mail e INNER JOIN mail2 d on e.srcuser = d.srchost2 where e.srcuser = d.srchost2 and e.srcuser ='中文测试1' ;
SELECT * from mail e LEFT JOIN mail2 d on e.srcuser = d.srchost2 where e.srcuser = d.srchost2 and d.srchost2 ='中文测试1' ;
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srchost2 where e.srcuser = d.srchost2 and e.srcuser ='中文测试1' ;
SELECT * from mail e LEFT JOIN mail2 d on e.srcuser = d.srchost2 WHERE d.srchost2 is NULL;
SELECT e.srcuser from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srchost2 is NULL;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srchost2 is NULL and e.srcuser like '??%';
SELECT * from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL;
SELECT * from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL and d.srcuser2 like '中文测%' ;
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.dstuser2 WHERE e.srcuser is NULL;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.dstuser2 union
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 where e.srcuser = d.srchost2 and e.srcuser ='中文测试1' ;
SELECT count(*) from mail e LEFT JOIN mail2 d on e.srcuser = d.srchost2 WHERE d.srcuser2 is NULL union
SELECT count(*) from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL and e.srcuser like '??%';


SELECT * from mail e LEFT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE d.srcuser2 is NULL union
SELECT * from mail e RIGHT JOIN mail2 d on e.srcuser = d.srcuser2 WHERE e.srcuser is NULL and e.srcuser like '??%';


####函数比较||说明：OB oracle模式支持的表达式合集已单独列一个测试文件全量验证
SELECT t.srcuser from mail t where t.t LIKE '%\\';


###正则表达式
select * from mail t where regexp_like(t.srcuser,'.*');
select * from mail t where regexp_like(t.srcuser,'^中文');
select * from mail t where regexp_like(t.srcuser,'测试1$');
select * from mail t where regexp_like(t.srcuser,'测试');
select * from mail t where regexp_like(t.srcuser,'^..测试');
select * from mail t where regexp_like(t.srcuser,'^[测试]');


SELECT REGEXP_INSTR(t.srcuser, '中文') from mail  t WHERE t.t =1;
SELECT REGEXP_INSTR(t.srcuser, '中文',2) from mail  t WHERE t.t =1;
SELECT REGEXP_INSTR(t.srcuser, '中文{2}') from mail  t WHERE t.t =1;


SELECT REGEXP_REPLACE(t.srcuser, '中文', 'ENGLISH') from mail t WHERE t.t =1;

SELECT REGEXP_SUBSTR(t.srcuser, '[a-z]+') from mail t WHERE t.t =7;
SELECT REGEXP_SUBSTR(t.srcuser, '[a-z]+', 1, 3) from mail t WHERE t.t =7;

SELECT REGEXP_INSTR(t.srcuser, 'b') from mail t WHERE t.t =8;
###OBE-00600: internal error code, arguments: -5820, The regular expression was too complex and current library can't be parsed
###SELECT REGEXP_INSTR(t.srcuser, '????bxxx') from mail t WHERE t.t =8;
###SELECT REGEXP_INSTR(t.srcuser, '?b') from mail t WHERE t.t =8;
SELECT REGEXP_INSTR(t.srcuser, 'b',4) from mail t WHERE t.t =8;

SELECT REGEXP_INSTR(t.srcuser, 'b') from mail t WHERE t.t =9;
SELECT REGEXP_INSTR(t.srcuser, 'b') from mail t WHERE t.t =10;
SELECT REGEXP_INSTR(t.srcuser, 'b') from mail t WHERE t.t =11;

SELECT REGEXP_SUBSTR(t.srcuser, '.*', 2) from mail t WHERE t.t =9;
SELECT REGEXP_SUBSTR(t.srcuser, '.*', 2) from mail t WHERE t.t =10;

####字符集和函数结果的整理
select t.srcuser from mail t where upper(t.srcuser) = '中文测试1';
select t.srcuser from mail t where LOWER(t.srcuser) = '中文测试1';

--disable_warnings
--error 0,942
drop table mail CASCADE CONSTRAINTS purge;
--error 0,942
drop table mail2 CASCADE CONSTRAINTS purge;
--error 0,942
drop table mail3 CASCADE CONSTRAINTS purge;
--error 0,942
drop table foo CASCADE CONSTRAINTS purge;
--enable_warnings

####插入字段时，以函数数据插入比较

--echo  ##################select##################
--disable_warnings
--error 0,1418
DROP index name_index3;
--error 0,942
DROP TABLE nvarchar2_test1 CASCADE CONSTRAINTS purge;
--enable_warnings

create table nvarchar2_test1(s1 number , s2 nvarchar2(200), s3 nvarchar2(200), s4 nvarchar2(200), s5 nvarchar2(200), s6 nvarchar2(200),
constraint name_unique3 unique(s2,s3),
check(length(s5) > 5)) compress;

create index name_index3 on nvarchar2_test1(s2);

#comment on table nvarchar2_test1 is 'clob类型测试表';
#comment on column nvarchar2_test1.s1 is '作为primary key使用';
#comment on column nvarchar2_test1.s6 is '作为分组建来使用';

insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(10, '中文测试1', '2009-10-01', '1999/10/01 12:10:01', '19891001 10:00:01', '1');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(11, nvl('中文测试2', null), '2009-10-01', '1999/10/01 12:10:01', '19891001 10:00:01', '2');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(12, '中文测试3', decode('', null, '2009-10-01', '', '2000-10-10'), '1999/10/01 12:10:01', '19891001 10:00:01', '3');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(13, '20091001', '2009-10-01', substr('1999/10/01 06:00:00',1,20), '19891001 10:00:01', '4');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(14, '20091002', '2009-10-01', '1999/10/01 12:10:01', cast('19790321' as varchar2(10)), '5');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(15, lower('AAAAAAA'), upper('bbbbbb'), '199901', cast('19790321' as varchar2(10)), '6');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(16, lower('CCCCCCC'), upper('ffffff'), '199901', cast('19790321' as varchar2(10)), '6');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(17, lower('DDDDDDD'), upper('ggggggg'), '199901', cast('19790321' as varchar2(10)), '6');
insert into nvarchar2_test1(s1, s2, s3, s4, s5, s6) values(18, lower('EEEEEEE'), upper('oooo'), '199901', cast('19790321' as varchar2(10)), '6');
select * from nvarchar2_test1;

--echo  # select中使用内置函数及算数运算
select length(t.s2) s2 from nvarchar2_test1 t where t.s2 = '中文测试2';
select nvl(t.s2, null) s2 from nvarchar2_test1 t where t.s2 = 'aaaaaaa';
select * from nvarchar2_test1 where substr(s3, 1, 4) = 'FFFF';
select s4+654321 s4 from nvarchar2_test1 where s4 = '19790321';
select s5*0 s5 from nvarchar2_test1 where s4 = '199901';
select * from nvarchar2_test1 where upper(s2) = '中文测试1';
select * from nvarchar2_test1 where lower(s3) = 'bbbbbb';
select * from nvarchar2_test1 where nvl(s5,'none') = '19891001 10:00:01';
select * from nvarchar2_test1 where upper(nvl(s2,'none')) = '中文测试2';
select decode(s4, '', '空', '1999/10/01 12:10:01', s4) s4 from nvarchar2_test1;
select  * from nvarchar2_test1 where decode(s4, '', '空', '1999/10/01 12:10:01', s4) is NULL;
select * from nvarchar2_test1 where s1< 15 and length(s2)>=6 ;
select * from nvarchar2_test1 where to_number(s6)< 12 and length(s3)>=4;

--echo  # select中函数转换
select to_number(s6) s6 from nvarchar2_test1;
select to_char(s2) s2, to_char(s3) s3 from nvarchar2_test1;


# nvl2表达式
select  nvl2(s4,'none','空值') s4 from nvarchar2_test1;
select  nvl2(s4,'none',000) s4 from nvarchar2_test1;

select s1, to_timestamp(s2, 'yyyymmdd') from nvarchar2_test1 where to_number(s1)=13 and length(s3)>=4;
select s1, to_timestamp_tz(s2, 'yyyy/mm/dd hh:mi:ss') s2 from nvarchar2_test1 where to_number(s1)=13 and length(s3)>=4;

select s1, to_date(s2, 'yyyymmdd') s2 from nvarchar2_test1 where to_number(s1)=13 and length(s3)>=4;

##select s1, next_day(s2,'Monday') s2 from nvarchar2_test1 where to_number(s1)=13 and length(s3)>=4;
select * from nvarchar2_test1 where to_number(s1)=13 and length(s3)>=4;

#select to_number(s1) s1, last_day(s2)+16 s2  from nvarchar2_test1 where to_number(s1)=13 and length(s3)>=4;

select s6 from nvarchar2_test1 group by s6 having cast(s6 as number)=2;
select count(s6) from nvarchar2_test1 group by s6 having to_number(s6)=2;
select count(s6) from nvarchar2_test1;
select count(*) from nvarchar2_test1 group by s6;
select count(*) from nvarchar2_test1 group by s6 having to_number(s6)=2;


--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-9 
select * from nvarchar2_test1  order by s6;
--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-4 
select distinct s1 from nvarchar2_test1 order by s1;

--disable_warnings
--error 0,942
DROP TABLE nvarchar2_test1 CASCADE CONSTRAINTS purge;
--enable_warnings

###分析函数
--disable_warnings
--error 0,1418
DROP index name_index4;
--error 0,942
drop table emp CASCADE CONSTRAINTS purge;
--enable_warnings

CREATE TABLE emp (
  empno    NUMBER(4) PRIMARY KEY,
  ename    NVARCHAR2(10),
  job      VARCHAR2(9),
  mgr      NUMBER(4),
  hiredate DATE,
  sal      NUMBER(7,2),
  comm     NUMBER(7,2),
  deptno   NUMBER(2),
  constraint name_unique4 unique(ename,job)
);

--error 1408
create index name_index4 on emp(ename,job);

INSERT INTO emp VALUES (7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO emp VALUES (7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO emp VALUES (7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO emp VALUES (7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO emp VALUES (7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO emp VALUES (7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO emp VALUES (7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
###INSERT INTO emp VALUES (7788,'SCOTT','ANALYST',7566,to_date('13-JUL-87','dd-mm-rr')-85,3000,NULL,20);
INSERT INTO emp VALUES (7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO emp VALUES (7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
###INSERT INTO emp VALUES (7876,'ADAMS','CLERK',7788,to_date('13-JUL-87', 'dd-mm-rr')-51,1100,NULL,20);
INSERT INTO emp VALUES (7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO emp VALUES (7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO emp VALUES (7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);

SELECT AVG(sal)
FROM   emp;

SELECT deptno, AVG(sal)
FROM   emp
GROUP BY deptno
ORDER BY deptno;

SELECT empno, deptno, sal,
       AVG(sal) OVER (PARTITION BY deptno) AS avg_dept_sal
FROM   emp;

SELECT empno, deptno, sal,
       AVG(sal) OVER () AS avg_sal
FROM   emp;

SELECT empno, deptno, sal,
       AVG(sal) OVER (PARTITION BY deptno) AS avg_dept_sal
FROM   emp;

SELECT empno, deptno, sal, 
       FIRST_VALUE(sal IGNORE NULLS) OVER (PARTITION BY deptno) AS first_sal_in_dept
FROM   emp;

SELECT empno, deptno, sal, 
       FIRST_VALUE(sal IGNORE NULLS) OVER (PARTITION BY deptno ORDER BY sal ASC NULLS LAST) AS first_val_in_dept
FROM   emp;

--disable_warnings
--error 0,942
drop table emp CASCADE CONSTRAINTS purge;
--enable_warnings

#########ORACLE全量函数测试；
--disable_warnings
--error 0,1418
DROP index name_index5;
--error 0,942
drop table mail4 CASCADE CONSTRAINTS purge;
--enable_warnings

create table mail4
(
c0 int,
c1 nvarchar2(200) default 'zhongwences010',
c2 varchar2(20) not null,
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 char(200),
c8 raw(200)
);

create index name_index5 on mail4(c1);
insert into mail4 values(1,'中文测试001','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(2,'中文测试002','中文测试002','aaabbb','中文测试002',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'c','ee');
insert into mail4 values(3,'中文测试003','中文测试001','aaabbb','中文测试003',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'d','ff');
insert into mail4 values(4,'aaabbb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc');
insert into mail4 values(5,'a','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(6,'bb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(7,'20060604','20060604','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(8,'2006-06-04','aaabbb','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb');
insert into mail4 values(9,'中文测试009','aaabbb','200','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','200');
insert into mail4 values(10,'中文测试010','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','300');

###select * from mail4;
###ORACLE全量数据类型验证：不同字段类型相互比较，值相同
select * from mail4 t where t.c1 = t.c2;
--error 00932
select * from mail4 t where t.c1 = t.c3;
--error 00932
select * from mail4 t where t.c1 = t.c4;
--error 1858
select * from mail4 t where t.c1 = t.c5;
--error 1858
select * from mail4 t where t.c1 = t.c6;
select * from mail4 t where t.c1 = t.c7;
select * from mail4 t where t.c1 = t.c8;
###select * from mail4 t where t.c2 = t.c4;
select * from mail4 t where t.c2 = t.c7;
###select * from mail4 t where t.c2 = t.c3;


###ORACLE部分函数验证

select * from mail4 t where t.c1 = '中文测试001';
select * from mail4 t where t.c1 = 'aaabbb';
select * from mail4 t where t.c1 = '2006-06-04';
select * from mail4 t where t.c1 = 'a';

select ABS(to_number(c8)) from mail4 where c8 = '0200';

###select ACOS(to_number(c8)) from mail4 where c8 = '0200';

select ADD_MONTHS(c5,13) from mail4;
###OB设计如此
###select * from mail4 t where  ADD_MONTHS(c5,13) = '2007-07-04 00:00:00';
###select * from mail4 t where  ADD_MONTHS(c5,13) = '04-JUL-07';

select count(c1) from mail4;
select count(t.c1) cnt from mail4 t where c0 = 10 group by t.c1 ;

select ASCII(t.c7) from mail4 t;
select * from mail4 t where  ASCII(t.c7) = 100;

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-9 
SELECT c0, CAST(c1 AS VARCHAR2(30)) text
FROM mail4 where CAST(c1 AS VARCHAR2(30)) = '中文测试009'
ORDER BY c0;

SELECT c0, CAST(c2 AS VARCHAR2(30)) text
FROM mail4 where CAST(c2 AS VARCHAR2(30)) = '中文测试002'
ORDER BY c0;

SELECT c0, CAST(c2 AS VARCHAR2(30)) text
FROM mail4 where CAST(c2 AS VARCHAR2(30)) like '中文测试002%'
ORDER BY c0;

SELECT c0, CAST(c2 AS VARCHAR2(30)) text
FROM mail4 where CAST(c2 AS VARCHAR2(30)) like '中文测试002%' and CAST(c1 AS VARCHAR2(30)) like '中文测试002%'
ORDER BY c0;
  
select CEIL(t.c0) from mail4 t;
select * from mail4 t where  CEIL(t.c0) = 10;

select lengthb(chr(t.c0)) from mail4 t;
--error 1722
select * from mail4 t where  chr(t.c0) = 100;

###检查逻辑不一致
###select COALESCE(t.c0,to_number(t.c7),5) from mail4 t where t.c0 > 6;
###select to_number(t.c7) from mail4 t where t.c0 > 6;
###select * from mail4 t where  COALESCE(t.c0,to_number(t.c7),5) = 10;

###OB不支持，ORACLE支持
###select collect(t.c1) from mail4 t;
###select collect(distinct(t.c1)) from mail4 t ;

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-3 
select CONCAT(t.c0,t.c1) from mail4 t order by t.c0;
select CONCAT(t.c1,t.c2) from mail4 t order by t.c0;

###concat clob类型时，ORACLE支持，OB不支持，且OB报错信息有问题，报错为nlob了。||说明：暂不支持NClob，concat返回结果为NCLOB，所以此时OB错误结果符合预期。
###select CONCAT(t.c1,t.c4) from mail4 t order by t.c0;
###select dump(CONCAT(t.c1,t.c4)) from mail4 t;

select CONCAT(t.c1,t.c5) from mail4 t order by t.c0;
select CONCAT(t.c1,t.c7) from mail4 t order by t.c0;

select * from mail4 t where trim(CONCAT(t.c1,t.c7)) = '中文测试010a';

###OB语法暂不支持
###SELECT CONVERT('? ê í ? ? A B C D E ', 'US7ASCII', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'UTF8', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'AL32UTF8', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'AL32UTF8', 'UTF8') FROM DUAL; 

select CORR(t.c0,t.c1) from mail4 t where t.c0 =7;

select COS(t.c1) from mail4 t WHERE t.c0 =7;
select COVAR_POP(t.c0,t.c1) from mail4 t where t.c0 =7;
select COVAR_SAMP(t.c0,t.c1) from mail4 t where t.c0 =7;

SELECT CUME_DIST(15000, .05) WITHIN GROUP
  (ORDER BY T.C0, T.C1) "Cume-Dist of 15500" 
  FROM mail4 T;

SELECT t.c0, t.c1, t.c2, CUME_DIST() 
  OVER (PARTITION BY t.c1 ORDER BY t.c0) AS cume_dist
  FROM mail4 t
  WHERE t.c0  =7
  ORDER BY t.c0, t.c1, t.c2, cume_dist;
  
SELECT t.c0,
       DECODE (t.c1, '中文测试001', 'Southlake', 
                             '中文测试002', 'San Francisco', 
                             'aaabbb', 'New Jersey', 
                             '2006-06-04', 'Seattle',
                                'Non domestic') "Location" 
  FROM mail4 t
  WHERE t.c0 < 17
  ORDER BY t.c0, "Location";

select * from mail4 where exists (SELECT 
       DECODE (t.c1, '中文测试001', 'Southlake',
                             '中文测试002', 'San Francisco',
                             'aaabbb', 'New Jersey',
                             '2006-06-04', 'Seattle',
                                '中国的文字') "Location" 
  FROM mail4 t
);

select * from mail4 where not exists (SELECT 
       DECODE (t.c1, '中文测试001', 'Southlake',
                             '中文测试002', 'San Francisco',
                             'aaabbb', 'New Jersey',
                             '2006-06-04', 'Seattle',
                                '中国的文字') "Location" 
  FROM mail4 t
);

##BUG：
##--disable_warnings
##--error 0,942
##drop table testtable CASCADE CONSTRAINTS purge;
##--enable_warnings
##
##create table testtable as select * from (SELECT 
##       DECODE (t.c1, '中文测试001', 'Southlake',
##                             '中文测试002', 'San Francisco',
##                             'aaabbb', 'New Jersey',
##                             '2006-06-04', 'Seattle',
##                                '中国的文字') "Location"
##  FROM mail4 t
##);
##
##desc mail4;
##select * from testtable t where t.Location = '中国的文字';
##insert into testtable values ('01234567890123');


--echo  ##常规DML测试 gb18030-2 /INSERT gb18030-2-2//gb18030-2-2-1
--echo  ##常规DML测试 gb18030-2 /INSERT gb18030-2-2//gb18030-2-2-2
--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-1
--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-2
--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-3
--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-4
--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-1
--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-2
--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-3
--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-4

####dml补充测试:增删改查全量表达式验证

SELECT e.c1,count(e.c2) from mail4 e INNER JOIN mail4 d on e.c1 = d.c1 where e.c1 = d.c1 and e.c1 ='中文测试1' group by e.c1 having count(e.c2) > 1 order by e.c1;

--disable_warnings
--error 0,1418
DROP index name_index6;
--error 0,942
drop table mailbakup CASCADE CONSTRAINTS purge;
--enable_warnings

create table mailbakup
(
c0 int,
c1 varchar2(200),
c2 nvarchar2(20),
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 nchar(200),
c8 raw(200)
);


--echo  ##常规DML测试 gb18030-2 /INSERT gb18030-2-2//gb18030-2-2-1

insert into mailbakup select * from mail4;
create index name_index6 on mailbakup(c1,c7);
select * from mailbakup;

SELECT e.c1,count(e.c2) from mailbakup e INNER JOIN mailbakup d on e.c1 = d.c1 where e.c1 = d.c1 and e.c1 ='中文测试1' group by e.c1 having count(e.c2) > 1 order by e.c1;


--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-1 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-2 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-3 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-4 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-5 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-6 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-7 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-8 
--echo  ##常规DML测试 gb18030-2 /dml补充测试 gb18030-2-5//gb18030-2-5-9


update mailbakup t set t.c2 = '中文测试xxx' where t.c2 = '中文测试001';
update mailbakup t set t.c2 = (select e.c2 from mailbakup e where e.c1 = 'bb') where t.c1 = 'bb';
update mailbakup t set t.c2 = '中文测试yyy' where t.c1 in ('a','aaabbb');

update (select e.c1 c1old,e.c2 c2new from mailbakup e where e.c1 = 'bb') set c1old = c2new;
update mailbakup t set (t.c1,t.c2) = (select e.c1,e.c2 from mailbakup e where e.c1 = 'a') where t.c1 = 'a';

delete from (select * from mailbakup t where t.c1 = '中文测试001');
delete from mailbakup t where t.c1 in (select e.c2 from mailbakup e where e.c2 = '中文测试001');


--disable_warnings
--error 0,942
drop table mailbakup CASCADE CONSTRAINTS purge;
--enable_warnings


--disable_warnings
--error 0,942
drop table testtable CASCADE CONSTRAINTS purge;
--enable_warnings


--disable_warnings
--error 0,942
drop table mail4 CASCADE CONSTRAINTS purge;
--enable_warnings

--echo  ########视图中比较 ##########

--disable_warnings
--error 0,942
DROP TABLE EMP;
--error 0,942
DROP TABLE DEPT;
--enable_warnings

CREATE TABLE DEPT
(DEPTNO NUMBER(2),
	DNAME NVARCHAR2(100),
	LOC VARCHAR2(100) );
CREATE TABLE EMP
(EMPNO NUMBER(4),
	ENAME NVARCHAR2(100),
	JOB NVARCHAR2(100),
	MGR NUMBER(4),
	HIREDATE DATE,
	SAL NUMBER(7,2),
	COMM NUMBER(7,2),
	DEPTNO NUMBER(2));
INSERT INTO DEPT VALUES
	(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT VALUES (20,'RESEARCH','DALLAS');
INSERT INTO DEPT VALUES
	(30,'SALES','CHICAGO');
INSERT INTO DEPT VALUES
	(40,'OPERATIONS','BOSTON');
INSERT INTO DEPT VALUES
	(40,'中文测试001','BOSTON');
INSERT INTO DEPT VALUES
	(40,'中文测试001','BOSTON');
INSERT INTO DEPT VALUES
	(30,'中文测试002','中文测试002');	
INSERT INTO DEPT VALUES
	(30,'中文测试001','中文测试001');	
	
INSERT INTO EMP VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO EMP VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO EMP VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO EMP VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO EMP VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO EMP VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO EMP VALUES
(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO EMP VALUES
(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO EMP VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO EMP VALUES
(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20);
INSERT INTO EMP VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO EMP VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);
INSERT INTO EMP VALUES
(7955,'弥勒','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,30);
INSERT INTO EMP VALUES
(7966,'佛','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,40);

###对应测分
CREATE OR REPLACE VIEW EMPDETAIL
AS
SELECT EMPNO,ENAME,JOB,HIREDATE,EMP.DEPTNO,DNAME
FROM EMP JOIN DEPT ON EMP.DEPTNO=DEPT.DEPTNO;

--sorted_result
select * from EMPDETAIL;
select * from EMPDETAIL t where t.ENAME = '弥勒';

###对应测分oracle_gbk_nchar-1-1-1-1-5-2
CREATE OR REPLACE VIEW EMPDETAIL2
AS
SELECT EMP.*
FROM EMP,DEPT WHERE EMP.DEPTNO=DEPT.DEPTNO AND DEPT.DNAME = '中文测试001';

select * from EMPDETAIL2;
select * from EMPDETAIL2 t where t.ENAME = '弥勒';

CREATE OR REPLACE VIEW EMPDETAIL3
AS
SELECT EMP.*
FROM EMP,DEPT WHERE EMP.DEPTNO=DEPT.DEPTNO AND DEPT.DNAME = '中文测试002';

select * from EMPDETAIL3;
select * from EMPDETAIL3 t where t.ENAME = '弥勒';

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-7 
CREATE OR REPLACE VIEW dept_sum_vw(name,minsal,maxsal,avgsal)
     AS SELECT d.dname,min(e.sal),max(e.sal),avg(e.sal)
        FROM    emp e,dept d
        WHERE e.deptno=d.deptno
        GROUP BY d.dname;

###select * from dept_sum_vw;
select * from dept_sum_vw t where t.NAME = '中文测试001';

create or replace view testview2 as
select d.deptno,d.dname,nvl(e.num,'0')num from dept d,(select t.deptno,count(*) num  from emp t group by t.deptno)e
where d.deptno=e.deptno(+);
select * from testview2;
###select * from testview2 t where t.DNAME = '中文测试001';

--disable_warnings
--error 0,942
DROP TABLE EMP;
--error 0,942
DROP TABLE DEPT;
--enable_warnings


--disable_warnings
--error 0,942
drop table t1 CASCADE CONSTRAINTS purge;
--error 0,942
drop table t2 CASCADE CONSTRAINTS purge;
--error 0,942
drop table test1 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t_utf16 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t_utf16_tmp CASCADE CONSTRAINTS purge;
--error 0,1418
drop index t_utf16_idx_local;
--error 0,1418
drop index t_utf16_idx_global;
--error 0,942
drop view t_utf16_view;
--error 0,600
drop synonym t_utf16_synonym;
--error 0,600
drop public synonym t_utf16_synonym1;
--error 0,600
drop synonym t_utf16_synonym_v ;
--error 0,600
drop public synonym t_utf16_synonym_v1;
--error 0,600
drop synonym t_utf16_synonym_s;
--error 0,600
drop public synonym t_utf16_synonym_s1;
--enable_warnings


--echo #常见函数验证

create table t1(c1 varchar2(100) primary key,c2 varchar2(100),c3 varchar2(100));
insert into t1 values('测','测','测');
insert into t1 values('测试','测试','测试');
insert into t1 values('回归','回归','回归');
insert into t1 values('testtest','testtest','testtest');
commit;
select * from t1;
###select utl_raw.cast_to_raw(c1) from t1 order by c1;
###select utl_raw.cast_to_raw(c2) from t1 order by c1;
###select utl_raw.cast_to_raw(c3) from t1 order by c1;

select substr(c1,1,1),substr(c2,1,1),substr(c3,1,1) from t1 order by c1;
select upper(c1),upper(c2),upper(c3) from t1 order by c1;
select lower(c1),lower(c2),lower(c3) from t1 order by c1;

drop table t1;
create table t1(c1 varchar2(100),c2 varchar2(100),c3 varchar2(100));
insert into t1 values('测','测','测');
insert into t1 values('测试','测试','测试');
insert into t1 values('回归','回归','回归');
insert into t1 values('testtest','testtest','testtest');
insert into t1 values(null,null,null);
select nvl(c1,0),nvl(c2,0),nvl(c3,0) from t1 where c1 is null;
select nvl(c1,0),nvl(c2,0),nvl(c3,0) from t1 where c1 is not null;
select nvl(c1,'test'),nvl(c2,'test'),nvl(c3,'test') from t1 where c1 is null;
select nvl(c1,'test'),nvl(c2,'test'),nvl(c3,'test') from t1 where c1 is not null;
select nvl2(c1,1,0),nvl2(c2,1,0),nvl2(c3,1,0) from t1 where c1 is null;
select nvl2(c1,1,0),nvl2(c2,1,0),nvl2(c3,1,0) from t1 where c1 is not null;
select c1, lengthb(c1) from t1;
select c2, lengthb(c2) from t1;
select c3, lengthb(c3) from t1;
select c3, lengthb(cast(c3 as varchar2(100))) from t1;
select c3, lengthb(cast(c3 as varchar2(100))) from t1;
create table t2(c1 varchar2(100) primary key,c2 varchar2(100),c3 varchar2(100));
insert into t2 values('测','测','测');
insert into t2 values('测试','测试','测试');
insert into t2 values('你好','你好','你好');
insert into t2 values('test','test','test');
commit;
select * from t2;
###select utl_raw.cast_to_raw(c1),utl_raw.cast_to_raw(c2),utl_raw.cast_to_raw(c3) from t2;
select c1, lengthb(c1) from t2 order by c1;
select c2, lengthb(c2) from t2 order by c1;
select c3, lengthb(c3) from t2 order by c1;
select c3, lengthb(cast(c3 as varchar2(100))) from t2 order by c1;
select c3, lengthb(cast(c3 as varchar2(100))) from t2 order by c1;
--echo ## 测试join语句
select * from t1 inner join t2 on t1.c1 = t2.c1;


create table test1(c1 varchar2(100),c2 varchar2(100),c3 varchar2(100));
###comment on table test1 is '测试长度最长是多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少';
###comment on column test1.c1 is '测试长度最长是多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少';
###comment on column test1.c2 is '测试长度最长是多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少多少';
#select * from user_tab_comments where TABLE_NAME='TEST1';
#select * from user_tab_comments where comments is not null;
#select comments,lengthb(comments) from user_tab_comments where TABLE_NAME='TEST1';
#select * from user_col_comments where TABLE_NAME='TEST1';
#select * from user_col_comments where comments is not null;
#select comments,lengthb(comments) from user_col_comments where TABLE_NAME='TEST1';
insert into test1 values(100,100,100);
insert into test1 values('20190926','20190926','20190926');
insert into test1 values('??','??','??');
select to_number(c1),lengthb(to_number(c1)),lengthb(c1),to_number(c2),lengthb(to_number(c2)),lengthb(c2),to_number(c3),lengthb(to_number(c3)),lengthb(c3) from test1 where c1='100' or  c1='20190926';
##OB date相关函数处理nchar均正常处理，ORACLE有报错01861；OB做超集处理
select to_date(c1),lengthb(to_date(c1)),lengthb(c1),to_date(c2),lengthb(to_date(c2)),lengthb(c2),to_date(c3),lengthb(to_date(c3)),lengthb(c3) from test1 where c1='20190926';
select to_char(c1),lengthb(to_char(c1)),lengthb(c1),to_char(c2),lengthb(to_char(c2)),lengthb(c2),to_char(c3),lengthb(to_char(c3)),lengthb(c3) from test1 where c1='??';


--echo ###表达式###
--echo #关系运算符
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

create table t_utf16(c1 varchar2(20),c2 varchar2(20),c3 varchar2(20));
insert into t_utf16(c1,c2,c3) values(1,2,2);

select * from t_utf16 where c1<c2;
select * from t_utf16 where c2<c1;
select * from t_utf16 where c1>c1;
select * from t_utf16 where c2>c1;
select * from t_utf16 where c1=c2;

select * from t_utf16 where c1>=c2;
select * from t_utf16 where c2>=c1;
select * from t_utf16 where c2>=c3;
select * from t_utf16 where c2<=c1;

select * from t_utf16 where c1<=c2; 
select * from t_utf16 where c1!=c2;
select * from t_utf16 where c1<>c2;

###OB比较相等有结果。ORACLE没有
select * from t_utf16 where c2=c3;
select * from t_utf16 where c2<=c3;
select * from t_utf16 where c2!=c3;
select * from t_utf16 where c2<>c3;

--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(20),c3 nvarchar2(20));
insert into t_utf16(c1,c2,c3) values(1,2,2);


select * from t_utf16 where c1<c2;
select * from t_utf16 where c2<c1;
select * from t_utf16 where c1>c1;
select * from t_utf16 where c2>c1;
select * from t_utf16 where c1=c2;

select * from t_utf16 where c1>=c2;
select * from t_utf16 where c2>=c1;
select * from t_utf16 where c2>=c3;
select * from t_utf16 where c2<=c1;

select * from t_utf16 where c1<=c2; 
select * from t_utf16 where c1!=c2;
select * from t_utf16 where c1<>c2;

select * from t_utf16 where c2=c3;
select * from t_utf16 where c2<=c3;
select * from t_utf16 where c2!=c3;
select * from t_utf16 where c2<>c3;


--echo #逻辑运算符和范围运算符
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(20),c3 varchar2(20));
insert into t_utf16(c1,c2,c3) values(1,2,null);
insert into t_utf16(c1,c2,c3) values('ab','bc','abc');

select * from t_utf16 where c1 in ('4','5');
select * from t_utf16 where c1 in ('1','2');
select * from t_utf16 where c1 not in ('4','5');
select * from t_utf16 where c1 not in ('1','2');
select * from t_utf16 where c3 is null;
select * from t_utf16 where c2 is null;
select * from t_utf16 where c3 is not null;
select * from t_utf16 where c2 is not null;
select * from t_utf16 where c1=any('1','2','3');
select * from t_utf16 where c1=any('4','5');    
select * from t_utf16 where c1=all('1','2','3');
select * from t_utf16 where c1=all('1','1');
select * from t_utf16 where c1='1' and c2='2';
select * from t_utf16 where c1='1' and c2='3';
select * from t_utf16 where c1='1' or c2='2';
select * from t_utf16 where c1='2' or c2='2';
select * from t_utf16 where c1='1' or c2='1';
select * from t_utf16 where c1='2' or c2='1';
select * from t_utf16 where c1 between 'a' and 'b';
select * from t_utf16 where c1 between 'c' and 'd';


--echo #条件表达式：case和decode
--echo #case
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(10),c2 varchar2(10));
insert into t_utf16(c1,c2) values('a1',1);
insert into t_utf16(c1,c2) values('a2',2);
insert into t_utf16(c1,c2) values('a3',3);
insert into t_utf16(c1,c2) values('a4',4);
insert into t_utf16(c1,c2) values('a5',5);

select SUM(c2),
case c1
when cast('a1' as varchar2(10)) then cast('E' as varchar2(10))
when cast('a2' as varchar2(10)) then cast('E' as varchar2(10))
when cast('a3' as varchar2(10)) then cast('F' as varchar2(10))
when cast('a4' as varchar2(10)) then cast('F' as varchar2(10))
else cast('G' as varchar2(10)) end
from t_utf16
group by
case c1
when cast('a1' as varchar2(10)) then cast('E' as varchar2(10))
when cast('a2' as varchar2(10)) then cast('E' as varchar2(10))
when cast('a3' as varchar2(10)) then cast('F' as varchar2(10))
when cast('a4' as varchar2(10)) then cast('F' as varchar2(10))
else cast('G' as varchar2(10)) end;

select SUM(c2),
case c2
when cast('1' as varchar2(10)) then cast('E' as varchar2(10))
when cast('2' as varchar2(10)) then cast('E' as varchar2(10))
when cast('3' as varchar2(10)) then cast('F' as varchar2(10))
when cast('4' as varchar2(10)) then cast('F' as varchar2(10))
else cast('G' as varchar2(10)) end
from t_utf16
group by
case c2
when cast('1' as varchar2(10)) then cast('E' as varchar2(10))
when cast('2' as varchar2(10)) then cast('E' as varchar2(10))
when cast('3' as varchar2(10)) then cast('F' as varchar2(10))
when cast('4' as varchar2(10)) then cast('F' as varchar2(10))
else cast('G' as varchar2(10)) end;

  
--echo #decode
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(10),c2 varchar2(10));
insert into t_utf16(c1,c2) values('a1',1);
insert into t_utf16(c1,c2) values('a2',2);
insert into t_utf16(c1,c2) values('a3',1);
insert into t_utf16(c1,c2) values('a4',2);
insert into t_utf16(c1,c2) values('a5',3);
insert into t_utf16(c1,c2) values('    ',3);
insert into t_utf16(c1,c2) values('       ',3);
insert into t_utf16(c1,c2) values('          ',3);
select c1, decode(c2,1,'A',2,'B','C') as c2 from t_utf16;
select c1,lengthb(c1),cast(c1 as char(20)),lengthb(cast(c1 as char(20))) from t_utf16;
select c1,lengthb(c1),cast(c1 as varchar2(20)),lengthb(cast(c1 as varchar2(20))) from t_utf16;
select c2,lengthb(c2),cast(c2 as char(20)),lengthb(cast(c2 as char(20))) from t_utf16;
select c2,lengthb(c2),cast(c2 as varchar2(20)),lengthb(cast(c2 as varchar2(20))) from t_utf16;

--echo #order by , group by
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(20),c3 varchar2(20));
insert into t_utf16(c1,c2,c3) values(1,2,3);
insert into t_utf16(c1,c2,c3) values('ab','bc','abc');
select * from t_utf16 order by c1 desc;
select * from t_utf16 order by c2 desc;
select * from t_utf16 order by c3 desc;
select c1 as c2 from t_utf16 group by c1;
select c2 as c3 from t_utf16 group by c2;
select c3 as c1 from t_utf16 group by c3;
select c1 from t_utf16 where lengthb(c1)>lengthb(c2) group by c1 having c1!='a';
select c2 from t_utf16 where lengthb(c1)>lengthb(c3) group by c2 having c2!='a';
select c3 from t_utf16 where lengthb(c2)=lengthb(c2) group by c3 having c3!='a';
select c1,c2 from t_utf16 where lengthb(c1)>lengthb(c2) group by c1,c2 having c1!='a';
select c2,c3 from t_utf16 where lengthb(c1)>lengthb(c3) group by c2,c3 having c2!='a';
select c3,c1 from t_utf16 where lengthb(c2)=lengthb(c2) group by c3,c1 having c3!='a';

--echo ###空串和null###
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(a varchar2(30),b varchar2(30),c varchar2(30 byte),d varchar2(30 char));
insert into t_utf16(a,b) values('','');
select * from t_utf16 where a is null;
select * from t_utf16 where b is null;
select * from t_utf16 where c is null;
select * from t_utf16 where d is null;
--error 0,900
select * from t_utf16 where a is '';
--error 0,900
select * from t_utf16 where b is '';
--error 0,900
select * from t_utf16 where c is '';
--error 900
select * from t_utf16 where d is '';
select * from t_utf16 where a=c;
select * from t_utf16 where c=a;
select * from t_utf16 where b=d;
select * from t_utf16 where d=b;
select * from t_utf16 where a=b;
select * from t_utf16 where c=d;
select * from t_utf16 where a='';
select * from t_utf16 where c='';
select * from t_utf16 where b='';
select * from t_utf16 where d='';

--echo #trim、ltrim、rtrim(ltrim/rtrim多参数暂不支持)
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(100),c2 varchar2(1));
insert into t_utf16(c1,c2) values('asda','a');
select trim(leading c2 from c1) from t_utf16;
select trim(trailing c2 from c1) from t_utf16;
select trim(both c2 from c1) from t_utf16;
select * from t_utf16 where trim(both c2 from c1)<>'sda';
select trim(both c2 from c1) from t_utf16;

--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(100),c2 varchar2(100));
insert into t_utf16(c1,c2) values('aaaaminb','minb');
insert into t_utf16(c1,c2) values('12123456','12');
insert into t_utf16(c1,c2) values(' asd ',' a s d ');
select trim(c1),trim(c2) from t_utf16;
select ltrim(c1),ltrim(c2) from t_utf16;
select rtrim(c1),rtrim(c2) from t_utf16;
select * from t_utf16 order by trim(c1);
--error 0,600,30001
select trim(both c2 from c1) from t_utf16;

--echo #concat
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 char(100),c2 varchar2(50),c3 varchar2(20),c4 varchar2(50));
insert into t_utf16(c1,c2,c3,c4) values('123abc','456def','7890ghijklmn','saeqws');
insert into t_utf16(c1,c2,c3,c4) values('123','456','789','0ab');
select concat(c1,c2) from t_utf16;
select c1||c2 from t_utf16;
select concat(concat(c1,c2),c3) from t_utf16;
select c1||c2||c3 from t_utf16; 
select concat(concat(c1,c2),concat(c3,c4)) from t_utf16;
select c1||c2||c3||c4 from t_utf16;
select * from t_utf16 where concat(c1,c2)<>'123';
select * from t_utf16 order by concat(c1,c2);

--echo #rpad、lpad
--echo #nchar和nchar类型使用rpad/lpad查询到的数据错误并且会报4016，作为where和order by条件时报4012 bug链接: 
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(20));
insert into t_utf16(c1,c2) values('abcde1wqeqw几个啊','12345678901324567890');
insert into t_utf16(c1,c2) values('123','456');
select lpad(c1,0) from t_utf16;
select lpad(c1,1,c2) from t_utf16;
select lpad(c1,19,c2) from t_utf16;
select lpad(c1,21,c2) from t_utf16; 
select lpad(c1,30) from t_utf16;
select lpad(c1,50,c2) from t_utf16;
select rpad(c1,0) from t_utf16;
select rpad(c1,1,c2) from t_utf16;
select rpad(c1,19,c2) from t_utf16;
select rpad(c1,21,c2) from t_utf16;
select rpad(c1,30) from t_utf16;
select rpad(c1,50,c2) from t_utf16;
select * from t_utf16 where lpad(c1,1,c2)='a';
select * from t_utf16 order by rpad(c1,21,c2);

--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(20),c3 varchar2(20));
insert into t_utf16(c1,c2,c3) values('sgt','几个菜','123@#$');
insert into t_utf16(c1,c2) values('123','456');
select c1,lower(c1),upper(c1) from t_utf16;
select c2,lower(c2),upper(c2) from t_utf16;
select c3,lower(c3),upper(c3) from t_utf16;
select * from t_utf16 where lower(c1)='sgt' or upper(c1)='SGT';
select * from t_utf16 order by lower(c1);

--echo #substr、substrb(暂时不支持)
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(200));
insert into t_utf16(c1,c2) values('12345abcde','几个菜啊就喝成这样');
insert into t_utf16(c1,c2) values('123','456');
select substr(c1,2),substr(c2,2) from t_utf16;
select substr(c1,-15),substr(c2,-15) from t_utf16;
select substr(c1,2,5),substr(c2,2,5) from t_utf16;
select substr(c1,2,20),substr(c2,2,20) from t_utf16;
select substr(c1,-15,5),substr(c2,-15,5) from t_utf16;
select substr(c1,-15,20),substr(c2,-15,20) from t_utf16;
select * from t_utf16 where substr(c1,2,5)='2345a' and substr(c2,2,5)='个菜啊就喝';
select * from t_utf16 order by substr(c1,2,5);

--echo #length、lengthb
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(20),c3 varchar2(200));
insert into t_utf16(c1,c2,c3) values('1234567','abcdefg','几个菜啊');
insert into t_utf16(c1,c2) values('123','456');
select length(c1),length(c2),length(c3) from t_utf16;
select lengthb(c1),lengthb(c2),lengthb(c3) from t_utf16;
select * from t_utf16 where length(c1)=lengthb(c1) or length(c2)=lengthb(c2) or length(c3)=lengthb(c3);
select * from t_utf16 where length(c1)<>lengthb(c1) or length(c2)<>lengthb(c2) or length(c3)<>lengthb(c3);
select * from t_utf16 where length(c3)='12';
select * from t_utf16 order by length(c3);

--echo #instr
--disable_warnings
--error 0,600,942
drop table t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table t_utf16(c1 varchar2(20),c2 varchar2(20));
insert into t_utf16 values('helloworld','123456');
insert into t_utf16 values('123456','helloworld');
select instr(c1,'l') from t_utf16;
select instr(c2,'l') from t_utf16;
select instr(c1,'lo') from t_utf16;
select instr(c2,'lo') from t_utf16;
select instr(c1,'s') from t_utf16;
select instr(c2,'s') from t_utf16;
select instr(c1,'l',2) from t_utf16;
select instr(c2,'l',2) from t_utf16;
select instr(c1,'l',2,2) from t_utf16;
select instr(c2,'l',2,2) from t_utf16;
--error 1428
select instr(c1,'l',2,-1) from t_utf16;
--error 1428
select instr(c2,'l',2,-1) from t_utf16;
select instr(c1,'l',-4) from t_utf16;
select instr(c2,'l',-4) from t_utf16;
select instr(c1,'l',-4,2) from t_utf16;
select instr(c2,'l',-4,2) from t_utf16;
select * from t_utf16 where instr(c1,'l',2,2)='0';
select * from t_utf16 where instr(c2,'l',2,2)='0';
select * from t_utf16 order by instr(c1,'l',2);
select * from t_utf16 order by instr(c2,'l',2);

--echo #### 对象 create type is as object
delimiter /;
create or replace type nchar_100_object as object(o1 varchar2(100),o2 varchar2(100),o3 varchar2(100),o4 varchar2(100),o5 varchar2(100),o6 varchar2(100),o7 varchar2(100),o8 varchar2(100),o9 varchar2(100),o10 varchar2(100));
/
create or replace type nchar_object01 as object(o1 varchar2(100));
/
delimiter ;/
--error 0,900
create table ttx of nchar_object01;
delimiter /;
create or replace type nchar_table is table of varchar2(100);
/
create or replace type nchar_table is table of varchar2(100);
/
delimiter ;/

--disable_warnings
--error 0,600,942
DROP TABLE tt_nchar CASCADE CONSTRAINTS purge;
--enable_warnings
--error 0,900
create table tt_nchar(c1 varchar2(100), c2 nchar_table) NESTED TABLE c2 STORE as c2_nchar_tab;
--echo #### nest table: create type is as table
delimiter /;
create or replace TYPE nchar_nt1 AS TABLE OF varchar2(100);
/
--echo ### 在表中使用对象
create or replace type nchar_object_t01 as object(o1 varchar2(100));
/
--error 0,900
delimiter ;/
--error 0,900
create table ttt_nchar(c1 varchar2(100), c2 nchar_object_t01);

--echo ### 在表中使用数组
delimiter /;
create or replace type nchar_100_varray_t as varray(99) of varchar2(100);
/
delimiter ;/
--error 0,900
create table tttt_utf16_view_nchar(c1 varchar2(100), c2 nchar_100_varray_t);



--disable_warnings
--error 0,942
drop table t1 CASCADE CONSTRAINTS purge;
--error 0,942
drop table t2 CASCADE CONSTRAINTS purge;
--error 0,942
drop table test1 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t_utf16 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t_utf16_tmp CASCADE CONSTRAINTS purge;
--error 0,1418
drop index t_utf16_idx_local;
--error 0,1418
drop index t_utf16_idx_global;
--error 0,942
drop table tt_utf16_view;
--error 0,942
drop table ttt_utf16_view;
--error 0,942
drop view t_utf16_view;
--error 0,942
drop view tttt_utf16_view_nchar;
--error 0,942
drop view ttx;
--error 0,942
drop view ttt_nchar;
--error 0,600
drop synonym t_utf16_synonym;
--error 0,600
drop public synonym t_utf16_synonym1;
--error 0,600
drop synonym t_utf16_synonym_v ;
--error 0,600
drop public synonym t_utf16_synonym_v1;
--error 0,600
drop synonym t_utf16_synonym_s;
--error 0,600
drop public synonym t_utf16_synonym_s1;
--enable_warnings


--disable_warnings
--error 0,600,942
DROP TABLE tmp_table CASCADE CONSTRAINTS purge;
--error 0,600,942
DROP TABLE tmp2_table CASCADE CONSTRAINTS purge;
--error 0,1418
drop index t_tmp_idx_global_2;
--enable_warnings

#create table/index for dml

create table tmp_table (c1 varchar2(100), c2 varchar2(100), c3 varchar2(100), c4 varchar2(100), c5 varchar2(100), c6 nvarchar2(1000), c7 nvarchar2(1000), c8 nvarchar2(1000), c9 nvarchar2(1000), c10 nvarchar2(1000), c11 varchar2(1000), constraint tmp_table_pk_test primary key(c1), constraint tmp_table_unique_test unique(c3), constraint tmp_table_check_test check(floor(c4) >= 0)) pctfree 3  pctused 3 initrans 3  compress basic enable row movement partition by hash(c1);
create table tmp2_table (c1 nvarchar2(1000), c2 nvarchar2(1000), c3 nvarchar2(1000), c4 nvarchar2(1000), c5 nvarchar2(1000), c6 varchar2(100), c7 varchar2(100), c8 varchar2(100), c9 varchar2(100), c10 nvarchar2(1000), c11 varchar2(1000), constraint tmp2_table_pk_test primary key(c1), constraint tmp2_table_unique_test unique(c3), constraint tmp2_table_check_test check(floor(c4) >= 0)) pctfree 3  pctused 3 initrans 3  compress basic enable row movement partition by hash(c1);

create index t_tmp_idx_global_2 on tmp_table(floor(c1),floor(c2),floor(c5)) global;

#insert test
insert into tmp_table values(floor(1.2344545454e3), floor(3.3434343),floor(1.23234545454e4d), floor(1.34343343e5f),floor(7899.104),floor(23),floor(1.2344545454e3), floor(66666),floor(123445.54565), floor(111.44444),floor(2322.44545));
insert into tmp_table(c1,c3,c4,c7) values(floor(199.2344545454e3),floor(29d),floor(1.34343343e5f),floor(1.2344545454e3));

insert into tmp2_table(c1,c3,c4,c7) select floor(c1),floor(c3),floor(c4),floor(c7) from tmp_table;

###OB报错，ORACLE正常
##insert into tmp2_table values(floor(9.999999e10), floor(9.999999e100),floor(9.9999e307d), floor(9.999e37f), floor(9999999999999999999999999999999999999),floor(9999999999999999999999999999999999999),floor(9.999999e10),floor(9.999999e100),floor(9.999999e10),floor(9.999999e100),floor(9.999999e100));

--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-1
--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-2
--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-3
--echo  ##常规DML测试 gb18030-2 /UPDATE gb18030-2-3//gb18030-2-3-4
#update test
update tmp_table set c1 = floor(c1), c2 = floor(c2), c3 = floor(c3), c4 = floor(c4), c5 = floor(c5), c6 = floor(c6), c7 = floor(c7), c8 = floor(c8), c9 = floor(c9), c10 = floor(c10), c11 = floor(c11);

update tmp_table set c1 = floor(c1), c2 = floor(c2), c3 = floor(c3), c4 = floor(c4), c5 = floor(c5), c6 = floor(c6), c7 = floor(c7), c8 = floor(c8), c9 = floor(c9), c10 = floor(c10), c11 = floor(c11) where c1 = floor(1.2344545454e3);

update tmp2_table set (c3,c4) = (select floor(c3) + 0.1,floor(c4) + 0.1 from tmp_table where c1 = floor(1.2344545454e3) ) where c1 = floor(1.2344545454e3);

select * from tmp_table order by c1;
select * from tmp2_table order by c1;

--echo  ##常规DML测试 gb18030-2 /INSERT gb18030-2-2//gb18030-2-2-3
#merge into test
##merge into tmp_table t1 using tmp2_table t2 on (t1.c1 = t2.c1) WHEN MATCHED THEN UPDATE SET t1.c2 = floor(t2.c2), t1.c3 = floor(t2.c3), t1.c4 = floor(t2.c4), t1.c5 = floor(t2.c5), t1.c6 = floor(t2.c6),  c8 = floor(t2.c8), t1.c9 = floor(t2.c9), t1.c10 = floor(t1.c10), t1.c11 = floor(t2.c11) WHEN NOT MATCHED THEN insert (c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11) values(floor(t2.c1),floor(t2.c2),floor(t2.c3),floor(t2.c4),floor(t2.c5),floor(t2.c6),floor(t2.c7),floor(t2.c8),floor(t2.c9),floor(t2.c10),floor(t2.c11));
##merge into tmp_table t1 using tmp2_table t2 on (t1.c1 = t2.c1) WHEN MATCHED THEN UPDATE SET t1.c2 = floor(t2.c2), t1.c3 = floor(t2.c3), t1.c4 = floor(t2.c4), t1.c5 = floor(t2.c5), t1.c6 = floor(t2.c6),  t1.c8 = floor(t2.c8), t1.c9 = floor(t2.c9), t1.c10 = floor(t1.c10), t1.c11 = floor(t2.c11) WHEN NOT MATCHED THEN insert (t1.c1,t1.c2,t1.c3,t1.c4,t1.c5,t1.c6,t1.c7,t1.c8,t1.c9,t1.c10,t1.c11) values(floor(t2.c1),floor(t2.c2),floor(t2.c3),floor(t2.c4),floor(t2.c5),floor(t2.c6),floor(t2.c7),floor(t2.c8),floor(t2.c9),floor(t2.c10),floor(t2.c11));


--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-1
--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-2
--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-3
#delete
delete from tmp2_table where floor(c1) > 2;
commit;

set session autocommit = 1;
--disable_warnings
--error 0,600,942
DROP TABLE tmp_table CASCADE CONSTRAINTS purge;
--error 0,600,942
DROP TABLE tmp2_table CASCADE CONSTRAINTS purge;
--error 0,1418
drop index t_tmp_idx_global_2;
--enable_warnings

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-12
--echo  ##################select中使用内置函数处理
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu1;
--enable_warnings

--echo  # 测试check和默认值的冲突
create table stu1(s1 varchar2(2) primary key, s2 varchar2(6) not null, s3 varchar2(6) default '123456', s4 varchar2(6) default 123456, s5 varchar2(7) default 4567890, check(length(s5) > 6));
--error 2290
insert into stu1(s1,s2,s5) values(1, 11, 111111);

insert into stu1(s1,s2,s5) values(2, 11, 1111111);
--error 12899
insert into stu1(s1,s2,s5) values(3, 11, 11111111);

select * from stu1;
select length(s5) from stu1;
select lengthb(s5) from stu1;


create table stu(s1 varchar2(2) primary key, s2 varchar2(6) not null, s3 varchar2(6) default '123456', s4 varchar2(6) default 123456, s5 varchar2(7) default 4567890, s6 varchar2(1) default '1',check(length(s5) > 5)) compress;
##comment on table stu is 'nchar类型测试表';
##comment on column stu.s1 is '作为primary key使用';
##comment on column stu.s6 is '作为分组建来使用';

--echo  # insert中验证primary key, not null, check的约束作用
insert into stu(s1,s2) values(1, 11);
insert into stu(s1,s2,s5) values(2, 11, 111111);
###--error 00001
###insert into stu(s1,s2,s5) values(1, 11, 111111);
--error 01400
insert into stu(s1,s5) values(1, 111111);
--error 01400
insert into stu(s1,s5) values(2, 222222);
--error 12899
insert into stu(s1,s2,s5) values(2, 22, 22222222);
--error 01400
insert into stu(s2,s5) values(33, 333333);
insert into stu(s1,s2,s5,s6) values(3, 33, 333333, 3);

--echo  # dml全量验证在RQG全量覆盖
--echo  # select中使用内置函数及算数运算nchar_nvarchar2_1_1_1
select length(s1) s1, nvl(s2, null) s2,substr(s3, 1, 4) s3, s4+654321 s4, s5*0 s5 from stu;
select upper(s1) s1, lower(s2) s2, nvl(s6,'none') s6 from stu;
select upper(s1) s1, lower(s2) s2, upper(nvl(s6,'none')) s6 from stu;
select upper(s1) s1, lower(s2) s2, decode(s6, null, '空', 3, s6) s3, nvl(s6,'none') s6 from stu;
select upper(s1) s1, lower(s2) s2, decode(s6, null, '空', s6, s6) s3, nvl(s6,'none') s6 from stu;
select upper(s1) s1, lower(s2) s2, decode(s6, '', '空', s6, s6) s3, nvl(s6,'none') s6 from stu;
select upper(s1) s1, lower(s2) s2, decode(s6, '', '空', s6, s6) s3, nvl2(s6,'none','空值') s6 from stu;
update stu set s6='' where s6=3;
select upper(s1) s1, lower(s2) s2, decode(s6, '', '空', s6, s6) s3, nvl2(s6,'none',000) s6 from stu;

--echo  # select中函数转换
select to_number(s1) s1, to_number(s2) s2, to_number(s3) s3, to_number(s4) s4, to_number(s5) s5 from stu;
select to_char(s1) s1, to_char(s2) s2, to_char(s3) s3, to_char(s4) s4, to_char(s5) s5 from stu;
select cast(s1 as number) s1, cast(s2 as number) s2, cast(s3 as number) s3, cast(s4 as number) s4, cast(s5 as number) s5 from stu;
--echo  ##常规DML测试 gb18030-2 /DELETE gb18030-2-4//gb18030-2-4-4
delete from stu where 1=1;

--echo  # 修改字段类型, 测试函数及分组等
--echo  # 修改为比已有内容少
--echo  # 目前不支持alter操作 
--error 12899
alter table stu modify (s2 varchar2(1), s3 varchar2(1), s4 varchar2(1), s5 varchar2(1));
alter table stu modify (s2 varchar2(20), s3 varchar2(20), s4 varchar2(20), s5 varchar2(20));

--echo  # 删表重新创建, 因不支持alter操作
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu1;
--enable_warnings
create table stu(s1 varchar2(2) primary key, s2 varchar2(20) not null, s3 varchar2(20) default '123456', s4 varchar2(20) default 123456, s5 varchar2(20) default 45678, s6 varchar2(1) default '1',check(length(s5) > 5)) compress;

insert into stu(s1, s2, s3, s4, s5, s6) values(10, 20191001, 2009-10-01, '1999/10/01 12:10:01', '19891001 10:00:01', 2);
insert into stu values(11, to_number('18600618'), 1875-09-24, '1987/05/21 08:23:00', cast('19790321' as number), 2);
insert into stu values(12, nvl('20191001', null), 2009-10-01, substr('1999/10/01 06:00:00',1,20), '19891001', length('aa'));
insert into stu values(13, nvl('18191221', null), decode('', null, '2009-10-01', '', '2000-10-10'), substr('1999-10-01 01:10:59',1,20), '19891001', length('aab'));
insert into stu values(14, to_number('18600618'), 1875-09-24, '1987/05/21 11:38:11', cast('19790321' as number), '2');
###--error 00001
###insert into stu values(14, to_number('18600618'), 1875-09-24, '1987/05/21', cast('19790321' as number), '2');

--echo  # where中使用函数, select中值转为时间类型
select * from stu where s1=13;
select * from stu where s6=2;
select * from stu where cast(s1 as number)=11;
select * from stu where to_number(s1)< 5 and length(s2)>=6 or 1=1;
select * from stu where to_number(s1)< 12 and length(s3)>=4;

select s1, to_timestamp(s2, 'yyyymmdd') from stu where to_number(s1)<=13 and length(s3)>=4;
ALTER SESSION SET TIME_ZONE='+08:00';
select s1, to_timestamp(s2, 'yyyymmdd') s2, to_timestamp_tz(s4, 'yyyy/mm/dd hh:mi:ss') s4 from stu where to_number(s1)<13 and length(s3)>=4;
select s1, to_date(s2, 'yyyymmdd') s2, to_timestamp(s2, 'yyyymmdd') s3 from stu where to_number(s1)<=13 and length(s3)>=4;
--error 01858
select s1, to_date(s2, 'yyyymmdd') s2, to_timestamp(s2, 'yyyymmdd') s3, to_timestamp_tz(s4, 'yyyymmdd hh:mi:ss') as s4 from stu where to_number(s1)<=13 and length(s3)>=4;
select s1, to_date(s2, 'yyyymmdd') s2, to_timestamp(s2, 'yyyymmdd') s3, to_timestamp_tz(s4, 'yyyy-mm-dd hh:mi:ss') as s4 from stu where to_number(s1)=13 and length(s3)>=4;
--error 01840
select s1, next_day(s2,'Monday') s2, next_day(s3,'Tuesday') s3, next_day(s4,'Wednesday') s4, next_day(s5,'Thursday') s5 from stu where to_number(s1)!=1 and length(s3)>=4;
##OB的超集，支持此种写法
##select s1, next_day(s2,'Monday') s2, next_day(s4,'Wednesday') s4, next_day(s5,'Thursday') s5 from stu ;
##select s1, next_day(s2,'Sunday')-28 s2, next_day(s4,'Friday')+12 s4, next_day(s5,'Saturday')-31 s5 from stu;
##select to_number(s1) s1, last_day(s2)+16 s2, last_day(s4)-30 s4, last_day(s5)+28 s5 from stu;

--echo  # sql语法中的综合使用
select s6 from stu group by s6 having cast(s6 as number)=2;
select count(s6) from stu group by s6 having to_number(s6)=2;
select s1, s2, s3, s4, s5, s6 from stu where to_number(s1)>9 and cast(s6 as number) in (2,3) order by s1,s6;
select distinct s6, s1 from stu where to_number(s1)>9 and cast(s6 as number) in (2,3) order by s1,s6;


--disable_warnings
--disable_query_log
--error 0, 942
drop table  simple_nchar;
--enable_warnings
--enable_query_log

--echo  # as列别名
 

create table simple_nchar(c0 int, c1 varchar2(10), c2 varchar2(10), c3 char(10), c4 char(10));
desc simple_nchar;

insert into simple_nchar values(1, 'a ', 'a ', 'a ', 'a ');

insert into simple_nchar values(2, 'a         ', 'a  ', 'a ', 'a  ');

select c0, length(c1), length(c2), length(c3), length(c4) from simple_nchar;

--echo #varchar2 vs varchar2
select * from simple_nchar where c1 = c2;
--echo #varchar2 vs char
select * from simple_nchar where c1 = c3;
--echo #char vs char
select * from simple_nchar where c3 = c4;

--echo #varchar2 vs const char
select * from simple_nchar where c2 = 'a ';

--echo #char vs const char
select * from simple_nchar where c3 = 'a ';

--echo #const char as select item
select * from (select 'a' as "a  " , 'a ' as "a" from dual) where "a  " ="a";
select length("a  "), length("a") from (select 'a' as "a  " , 'a ' as "a" from dual) where "a  " ="a";
select length(a), length(b) from (select 'a' as a , 'a ' as b from dual) where a = b;

--echo #const char as input param
select length('a ') from dual;

--echo #const char cast
--error 0,942
drop table molly_cast_t;
create table molly_cast_t as select cast('m' as char) as c from dual;
desc molly_cast_t;
select * from molly_cast_t where c = cast('m' as char);
drop table molly_cast_t;

--echo #const char in cycle clause
--disable_warnings
--disable_query_log
--error 0, 942
drop table  info;
--enable_warnings
--enable_query_log
create table info(pno int, no int, name varchar2(10));
insert into info values(0, 0, 'human');

--echo  # with as的使用
--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-2 
with  c(pno, no, name) as (select pno, no, name from info where name = 'eye' union all select b.pno, b.no, b.name from c, info b where c.pno = b.no) cycle pno set cyclemarker to 'y' default 'n' select c.*, cyclemarker from c where cyclemarker = 'y  ';

with  c(pno, no, name) as (select pno, no, name from info where name = 'human' union all select b.pno, b.no, b.name from c, info b where c.pno = b.no) cycle pno set cyclemarker to 'y' default 'n' select c.*, cyclemarker from c where cyclemarker = 'y';

--echo #empty_string
insert into simple_nchar values(3, '', '', '', '');
insert into simple_nchar values(4, '', '', '', '');

select * from simple_nchar where c1 is null and c2 is null and c3 is null and c4 is null;
select * from simple_nchar where '' is null;

--echo #
--error 0,942
drop table molly_empty_item;
--error 1723
create table molly_empty_item as select null as a from dual;
--error 1723
create table molly_empty_item(c1 char(0 byte));
--error 1723
create table molly_empty_item(c1 varchar2(0));
--echo #
select * from (select '' a from dual) where a is null;
select count(*) from (select '' a from dual) where a is null;


--echo  # group by/distinct等select综合应用

--error 600,4002
select concat(null) from dual;
--echo #
select concat(col,col) from ( select '' col from dual);
--echo #
--error 0,942
drop table bug18221254;
create table bug18221254(col varchar2(10) default '' not null, col1 int);
--error 1400
insert into bug18221254(col1) values(1);
drop table bug18221254;

--echo #group by
insert into simple_nchar values(5, 'a         ', 'a  ', 'a ', 'a  ');
select count(*), count(c1) , c2, length(c2) from simple_nchar group by c2;
select count(*), count(c2) , c3, length(c3) from simple_nchar group by c3;

--echo #distinct
select distinct c1 from simple_nchar;
select distinct c3 from simple_nchar;

create index idx_sv on simple_nchar(c1);
--echo #distinct
insert into simple_nchar(c0, c1) values(6, 'a ');
insert into simple_nchar(c0, c1) values(8, 'a ');
insert into simple_nchar(c0, c1) values(7, 'a ');
select distinct c1 from simple_nchar;

select length(c1) from simple_nchar order by c1;
select length(c2) from simple_nchar order by c2;

drop table simple_nchar;

--disable_warnings
--error 0, 942
drop table  t1;
--error 0, 942
drop table  t2;
--error 0, 942
drop table  t3;
--enable_warnings

--error 0,910
create table t1 (c1 varchar2(2001));
--error 900
create table t1 (c1 varchar2(-1));

create table t2 (c1 varchar2(2000));
create table t3 (c1 varchar2(10));

--echo  # 字段插入边界值验证

desc t1;
desc t2;
desc t3;

###--error 0,12899
###insert into t1 values ('但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但但');

###--error 12899
###insert into t2 values ('但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但但');
###--error 12899
###insert into t3 values ('但事实上但事实上但事实上但事实上但事实上但事实上但事实');

insert into t1 values ('但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但');
insert into t2 values ('但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但事实上但');
insert into t3 values ('但事实');
select length(c1) , lengthb(c1) from t1;
select length(c1) , lengthb(c1) from t2;
select length(c1) , lengthb(c1) from t3;

alter table t1 add c2 varchar2(2001);
alter table t2 add c2 varchar2(2001);

desc t1;
desc t2;

drop table t1;
drop table t2;
drop table t3;

--echo #max len
--error 0, 942
drop table col_len;
--error 0, 942
drop table col_len2;

create table col_len(c1 varchar2(4001));
create table col_len2(c1 varchar2(10000));
--error 910
create table col_len(c1 varchar2(400100));


drop table col_len;
drop table col_len2;

--error 910
create table col_len(c1 char(2001));
--error 910
create table col_len(c1 char(20018943));

create table col_len(c1 char(1000));
drop table col_len;


--disable_warnings
--error 0, 942
drop table bug_nchar_t1;
--error 0, 942
drop table bug_nchar_t2;
--error 0, 942
drop table bug_nchar_t3;
--error 0, 942
drop table bug_nchar_t4;
--error 0, 942
drop table bug_nchar_t5;
--error 0, 942
drop table bug_nchar_t6;
--error 0, 942
drop table bug_nchar_t7;
--error 0, 942
drop table bug_nchar_t8;
--enable_warnings

--echo  #部分表达式验证，CAST AS /case when等/union all


#issue #18116362
--error 0, 942
drop table bug_nchar_t1;
create table bug_nchar_t1(c1 int,c2 varchar2(10));
insert into bug_nchar_t1 values(1,1);
insert into bug_nchar_t1 values(2,1);

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-11
select * from (
select CAST(A.name AS varchar2(23)) AS name from (
select case when c1=1 then 'INDEX' when c1=2 then 'TABLE' end as name from bug_nchar_t1
union all
select 'TABLE SUBPARTITION' as name from bug_nchar_t1
)a )where name ='INDEX';

select name from (select case when c1=1 then 'INDEX' when c1=2 then 'TABLE' end as name from bug_nchar_t1
union all
select 'TABLE SUBPARTITION' as name from bug_nchar_t1) where cast(name as varchar2(20)) = 'INDEX';

--echo  #CTAS 创建表等nchar_nvarchar2_1_1_7
--echo #case type
--error 0, 942
drop table bug_nchar_t2;
create table bug_nchar_t2 as select case when c1=1 then 'INDEX' when c1=2 then 'TABLE' end as name from bug_nchar_t1;
desc bug_nchar_t2;
--error 0, 942
drop table bug_nchar_t3;
create table bug_nchar_t3 as select case when c1=1 then 'IND' when c1=2 then 'TABLE' end as name from bug_nchar_t1;
desc bug_nchar_t3;

--echo #union/union all type
--error 0, 942
drop table bug_nchar_t4;
create table bug_nchar_t4 as select 'abc' as const_str from dual union select 'abcdef' from dual;
desc bug_nchar_t4;

--error 0, 942
drop table bug_nchar_t4;
create table bug_nchar_t4 as select 'abc' as abc from dual union select 'abcdef' as bcd from dual;
desc bug_nchar_t4;

select * from bug_nchar_t4;

--error 0, 942
drop table bug_nchar_t6;
create table bug_nchar_t6 as select 'abc' as const_str from dual union all select 'abcdef' from dual intersect select 'abc' from dual;
desc bug_nchar_t6;
select * from bug_nchar_t6;

--error 0, 942
drop table bug_nchar_t7;
create table bug_nchar_t7 as select 'abc' as const_str from dual union all select 'abcdef' from dual minus select 'abc' from dual;
desc bug_nchar_t7;
select * from bug_nchar_t7;

--error 0, 942
drop table bug_nchar_t8;
create table bug_nchar_t8 as select 'abc' as const_str from dual union all select 'def' from dual minus select 'def' from dual;
desc bug_nchar_t8;
select * from bug_nchar_t8;

--disable_warnings
--error 0, 942
drop table bug_nchar_t1;
--error 0, 942
drop table bug_nchar_t2;
--error 0, 942
drop table bug_nchar_t3;
--error 0, 942
drop table bug_nchar_t4;
--error 0, 942
drop table bug_nchar_t5;
--error 0, 942
drop table bug_nchar_t6;
--error 0, 942
drop table bug_nchar_t7;
--error 0, 942
drop table bug_nchar_t8;
--enable_warnings


--disable_warnings
--error 0, 942
drop table t1;
--enable_warnings

--echo  #部分表达式验证，CAST AS 


create table t1 (c0 int, c1 varchar2(40), c2 varchar2(40), c3 varchar2(40));

insert into t1 (c0, c1) values (1, '哈哈');
insert into t1 (c0, c1) values (-1, '哈哈哈哈');
insert into t1 (c0, c1) values (-1, '哈哈哈abc');

insert into t1 (c0, c1) values (2, cast('哈哈' as varchar2(10)));
insert into t1 (c0, c1) values (3, cast('哈哈哈哈' as varchar2(10)));
insert into t1 (c0, c1) values (4, cast('哈哈哈abc' as varchar2(10)));

insert into t1 (c0, c1) values (5, cast('哈哈' as varchar2(10 char)));
insert into t1 (c0, c1) values (-1, cast('哈哈哈哈' as varchar2(10 char)));
insert into t1 (c0, c1) values (-1, cast('哈哈哈abc' as varchar2(10 char)));

insert into t1 (c0, c1) values (6, cast('哈哈' as varchar2(40 byte)));
insert into t1 (c0, c1) values (-1, cast('哈哈哈哈' as varchar2(40 byte)));
insert into t1 (c0, c1) values (-1, cast('哈哈哈abc' as varchar2(40 byte)));

insert into t1 (c0, c2) values (7, '哈哈');
insert into t1 (c0, c2) values (8, '哈哈哈哈');
insert into t1 (c0, c2) values (9, '哈哈哈abc');

insert into t1 (c0, c2) values (10, cast('哈哈' as varchar2(10)));
insert into t1 (c0, c2) values (11, cast('哈哈哈哈' as varchar2(10)));
insert into t1 (c0, c2) values (12, cast('哈哈哈abc' as varchar2(10)));

insert into t1 (c0, c2) values (13, cast('哈哈' as varchar2(10 char)));
insert into t1 (c0, c2) values (14, cast('哈哈哈哈' as varchar2(10 char)));
insert into t1 (c0, c2) values (15, cast('哈哈哈abc' as varchar2(10 char)));

insert into t1 (c0, c2) values (16, cast('哈哈' as varchar2(40 byte)));
insert into t1 (c0, c2) values (17, cast('哈哈哈哈' as varchar2(40 byte)));
insert into t1 (c0, c2) values (18, cast('哈哈哈abc' as varchar2(40 byte)));
select * from t1 order by c0 asc;

insert into t1 (c0, c3) values (19, '哈哈');
insert into t1 (c0, c3) values (20, '哈哈哈哈');
insert into t1 (c0, c3) values (21, '哈哈哈abc');

insert into t1 (c0, c3) values (22, cast('哈哈' as varchar2(10)));
insert into t1 (c0, c3) values (23, cast('哈哈哈哈' as varchar2(10)));
insert into t1 (c0, c3) values (24, cast('哈哈哈abc' as varchar2(10)));

insert into t1 (c0, c3) values (25, cast('哈哈' as varchar2(10 char)));
insert into t1 (c0, c3) values (26, cast('哈哈哈哈' as varchar2(10 char)));
insert into t1 (c0, c3) values (27, cast('哈哈哈abc' as varchar2(10 char)));

insert into t1 (c0, c3) values (28, cast('哈哈' as varchar2(40 byte)));
insert into t1 (c0, c3) values (29, cast('哈哈哈哈' as varchar2(40 byte)));
insert into t1 (c0, c3) values (30, cast('哈哈哈abc' as varchar2(40 byte)));
select * from t1 order by c0 asc;
drop table t1;

select cast('哈' as varchar2(1)), length(cast('哈' as varchar2(1))), lengthb(cast('哈' as varchar2(1))) from dual;
select cast('哈哈' as varchar2(1 byte)), length(cast('哈哈' as varchar2(1 byte))), lengthb(cast('哈哈' as varchar2(1 byte))) from dual;
select cast('哈abc' as varchar2(1 byte)), length(cast('哈abc' as varchar2(1 byte))), lengthb(cast('哈abc' as varchar2(1 byte))) from dual;
select cast('abc哈' as varchar2(1 byte)), length(cast('abc哈' as varchar2(1 byte))), lengthb(cast('abc哈' as varchar2(1 byte))) from dual;

select cast('哈' as varchar2(1 char)), length(cast('哈' as varchar2(1 char))), lengthb(cast('哈' as varchar2(1 char))) from dual;
select cast('哈哈' as varchar2(1 char)), length(cast('哈哈' as varchar2(1 char))), lengthb(cast('哈哈' as varchar2(1 char))) from dual;
select cast('哈abc' as varchar2(1 char)), length(cast('哈abc' as varchar2(1 char))), lengthb(cast('哈abc' as varchar2(1 char))) from dual;
select cast('abc哈' as varchar2(1 char)), length(cast('abc哈' as varchar2(1 char))), lengthb(cast('abc哈' as varchar2(1 char))) from dual;

--disable_warnings
--error 0, 942
drop table t1;
--enable_warnings


create table t1 (c0 int, c1 varchar2(20), c2 varchar2(20), c3 varchar2(20), c4 varchar2(20));
insert into t1 values (1, 'a', 'a', 'a', 'a');
insert into t1 values (2, '哈', '哈', '哈', '哈');
insert into t1 values (3, '哈abc哈abc哈ab哈', '哈abc哈abc哈ab哈', '哈abc哈abc哈ab哈', '哈abc哈abc哈ab哈');

select CAST(C1 AS CHAR(20 char)), CAST(C2 AS CHAR(20 char)), CAST(C3 AS CHAR(20 char)), CAST(C4 AS CHAR(20 char)) from t1 order by c0;
select CAST(C1 AS CHAR(20 byte)), CAST(C2 AS CHAR(20 byte)), CAST(C3 AS CHAR(20 byte)), CAST(C4 AS CHAR(20 byte)) from t1 order by c0;
select CAST(C1 AS CHAR(20)), CAST(C2 AS CHAR(20)), CAST(C3 AS CHAR(20)), CAST(C4 AS CHAR(20)) from t1 order by c0;
select CAST(C1 AS VARCHAR2(20 char)), CAST(C2 AS VARCHAR2(20 char)), CAST(C3 AS VARCHAR2(20 char)), CAST(C4 AS VARCHAR2(20 char)) from t1 order by c0;

select CAST(C1 AS VARCHAR2(20 byte)), CAST(C2 AS VARCHAR2(20 byte)), CAST(C3 AS VARCHAR2(20 byte)), CAST(C4 AS VARCHAR2(20 byte)) from t1 order by c0;

select CAST(C1 AS VARCHAR2(20 byte)) from t1 order by c0;
select CAST(C1 AS VARCHAR2(20)), CAST(C2 AS VARCHAR2(20)), CAST(C3 AS VARCHAR2(20)), CAST(C4 AS VARCHAR2(20)) from t1 order by c0;

select CAST(C1 AS CHAR), CAST(C2 AS CHAR), CAST(C3 AS CHAR), CAST(C4 AS CHAR) from t1 order by c0;
select CAST(C2 AS VARCHAR2(20 char)), length(CAST(C2 AS VARCHAR2(20 char))), lengthb(CAST(C2 AS VARCHAR2(20 char))) from t1 order by c0;
drop table t1;

--echo  #部分表达式嵌套使用验证

--disable_warnings
--error 0,00942
drop table test_dml1;
--enable_warnings

create table test_dml1(c0 varchar2(200),c1 nvarchar2(200),c2 char(200),列3 int)partition by hash(c1);
insert into test_dml1 values('\x1abf4','\x1abf4','\x1abf4',1);
insert into test_dml1 values('','','',2);
insert into test_dml1 values(' 字龠 ',' 字龠 ',' 字龠 ',3);
insert into test_dml1 values(' 字
龠 ',' 字
龠 ',' 字
龠 ',4);
insert into test_dml1 values(' 字    龠 ',' 字 龠 ',' 字 龠 ',5);
insert into test_dml1 values('字!@#$%^&*()_+龠','字!@#$%^&*()_+龠','字!@#$%^&*()_+龠',6);
insert into test_dml1 values('十','十','十',90);
insert into test_dml1 values('〡','〡','〡',91);
insert into test_dml1 values('〢','〢','〢',92);
insert into test_dml1 values('〣','〣','〣',93);
insert into test_dml1 values('〤','〤','〤',94);
insert into test_dml1 values('〥','〥','〥',95);
insert into test_dml1 values('〦','〦','〦',96);
insert into test_dml1 values('〧','〧','〧',97);
insert into test_dml1 values('〨','〨','〨',98);
insert into test_dml1 values('〩','〩','〩',99);
insert into test_dml1 values('~','~','~',111);
insert into test_dml1 values('!','!','!',112);
insert into test_dml1 values('@','@','@',113);
insert into test_dml1 values('#','#','#',114);
insert into test_dml1 values('$','$','$',115);
insert into test_dml1 values('%','%','%',116);
insert into test_dml1 values('^','^','^',117);
insert into test_dml1 values('&','&','&',118);
insert into test_dml1 values('*','*','*',119);
insert into test_dml1 values('(','(','(',110);
insert into test_dml1 values(')',')',')',121);
insert into test_dml1 values('_','_','_',122);
insert into test_dml1 values('Q','Q','Q',123);
insert into test_dml1 values('P','P','P',124);
insert into test_dml1 values('{','{','{',125);
insert into test_dml1 values('}','}','}',126);
insert into test_dml1 values('|','|','|',127);
insert into test_dml1 values('+','+','+',128);
insert into test_dml1 values('|','|','|',129);
insert into test_dml1 values('/','/','/',120);
insert into test_dml1 values('`','`','`',130);
insert into test_dml1 values('1','1','1',131);
insert into test_dml1 values('-','-','-',132);
insert into test_dml1 values('=','=','=',133);
insert into test_dml1 values('\','\','\',134);
insert into test_dml1 values('<','<','<',135);
insert into test_dml1 values('>','>','>',136);
insert into test_dml1 values('?','?','?',137);
insert into test_dml1 values(',',',',',',138);
insert into test_dml1 values('.','.','.',139);
insert into test_dml1 values('/','/','/',140);
insert into test_dml1 values('.','.','.',141);
insert into test_dml1 values('.','.','.',142);
insert into test_dml1 values('+','+','+',143);
insert into test_dml1 values('-','-','-',144);
insert into test_dml1 values('*','*','*',145);
insert into test_dml1 values('/','/','/',146);
insert into test_dml1 values('''','''','''',147);
insert into test_dml1 values('\\','\\','\\',148);
insert into test_dml1 values('0xd1','0xd1','0xd1',149);
insert into test_dml1 values('0XD1','0XD1','0XD1',150);
insert into test_dml1 values('x616263FF','x616263FF','x616263FF',151);
select 列3 as 中文列测试,c0,c1,c2,lengthb(c0) varchar_c0_字节长度,lengthb(c1) varchar2_c1_字节长度,length(c2) char_c2_字节长度,length(列3) char_c3_字节长度,length(c0) 列0長度,length(c1) 列1長度,length(c2) 列2長度,length(列3) 列3長度 from test_dml1 order by 中文列测试;
--echo ############trim/ltrim/rtrim############
select rtrim(列3),trim(c1),trim(c2),trim(列3),ltrim(c0),ltrim(c1),ltrim(c2),ltrim(列3),rtrim(c0),rtrim(c1),rtrim(c2),trim(c0),length(trim(c0)),length(trim(c1)),length(trim(c2)),length(trim(列3)),length(ltrim(c0)),length(ltrim(c1)),length(ltrim(c2)),length(ltrim(列3)),length(rtrim(c0)),length(rtrim(c1)),length(rtrim(c2)),length(rtrim(列3)),lengthb(trim(c0)),lengthb(trim(c1)),lengthb(trim(c2)),lengthb(trim(列3)),lengthb(ltrim(c0)),lengthb(ltrim(c1)),lengthb(ltrim(c2)),lengthb(ltrim(列3)),lengthb(rtrim(c0)),lengthb(rtrim(c1)),lengthb(rtrim(c2)),lengthb(rtrim(列3)) from test_dml1 order by 列3;


--disable_warnings
--error 0,00942
drop table test_dml1;
--enable_warnings

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-10
--echo  #子查询
--echo cast test
--disable_warnings
--error 0,00942
drop table cast_test_table;
--error 0,00942
drop table cast_test_table1;
--enable_warnings
create table cast_test_table(c1 varchar2(50),字段2  number);
insert into cast_test_table values('士大夫',67);
insert into cast_test_table values('安\德森',63);
insert into cast_test_table values('201303第三方的',77);
insert into cast_test_table values('杜少府',68);
insert into cast_test_table values('公环銸境',97);
insert into cast_test_table values('试点乛范围',62);
insert into cast_test_table values('繁體字測試',87);
select * from cast_test_table;
###select cast(c1 as varchar2(2)) as empno from cast_test_table;
###select cast(c1 as varchar2(3)) as empno from cast_test_table;
select cast(c1 as char(2)) as empno from cast_test_table;
create table cast_test_table1(字段1   varchar2(40),字段2     int,c3       number);
insert into cast_test_table1 values('繁體字測試',1,'1000');
insert into cast_test_table1 values('出口i给v',2,'500');
insert into cast_test_table1 values('我欸如乘客',3,'100');
select * from cast_test_table1;
SELECT c.c1,c.字段2,b.字段1,b.c3 FROM (SELECT c.*,ROW_NUMBER() OVER(ORDER BY 字段2 DESC) rn FROM cast_test_table c) c ,(SELECT b.字段1,b.c3,ROW_NUMBER() OVER(ORDER BY b.字段1) rn FROM cast_test_table1 b ) b WHERE c.rn=b.rn;


--echo  #like验证nchar_nvarchar2_1_1_10


--disable_warnings
--error 0,00942
drop table cast_test_table;
--error 0,00942
drop table cast_test_table1;
--enable_warnings

--disable_warnings
--error 0,00942
drop table gbk_chinese_function;
--enable_warnings
create table gbk_chinese_function(c1 varchar2(2000));
insert into gbk_chinese_function values('永和九年，岁在癸丑，暮春之初，会于会稽山阴之兰亭，修禊事也。群贤毕至，少长咸集。此地有崇山峻岭，茂林修竹，又有清流激湍，映带左右，引以为流觞曲水，列坐其次。虽无丝竹管弦之盛，一觞一咏，亦足以畅叙幽情。
是日也，天朗气清，惠风和畅。仰观宇宙之大，俯察品类之盛，所以游目骋怀，足以极视听之娱，信可乐也。
夫人之相与，俯仰一世。或取诸怀抱，悟言一室之内；或因寄所托，放浪形骸之外。虽趣舍万殊，静躁不同，当其欣于所遇，暂得于己，快然自足，不知老之将至；及其所之既倦，情随事迁，感慨系之矣。向之所欣，俯仰之间，已为陈迹，犹不能不以之兴怀，况修短随化，终期于尽！古人云：“死生亦大矣。”岂不痛哉！
每览昔人兴感之由，若合一契，未尝不临文嗟悼，不能喻之于怀。固知一死生为虚诞，齐彭殇为妄作。后之视今，亦犹今之视昔，悲夫！故列叙时人，录其所述，虽世殊事异，所以兴怀，其致一也。后之览者，亦将有感于斯文。');
insert into gbk_chinese_function values('中文测试');
select c1,length(c1),lengthb(c1) from gbk_chinese_function;

--echo  ##常规DML测试 gb18030-2 /SELECT gb18030-2-1//gb18030-2-1-13

select * from gbk_chinese_function where c1 like '%select substr(c1,4,4) from gbk_chinese_function where rownum<2%';

--disable_warnings
--error 0,00942
drop table gbk_chinese_function;
--enable_warnings

--disable_warnings
--error 0,942
drop table mail;
--error 0,942
drop table mail2;
--error 0,942
drop table mail3;
--error 0,942
drop table foo;
--enable_warnings

--echo other function test
select length(concat('看v有-','么看完')||'23'),lengthb(concat('看v有-','么看完')||'23') from dual;
select lower('AaBbCcDd枯竭'),upper('AaBbCcDd枯竭'),lengthb(lower('AaBbCcDd枯竭')),length(lower('AaBbCcDd枯竭')) from dual;
select upper('哈哈') upper from dual;
select lower('哈哈') upper from dual;
select lpad(rpad('gao偶然客户',10,'*'),17,'*'),length(lpad(rpad('gao',10,'*'),17,'*')),lengthb(lpad(rpad('gao',10,'*'),17,'*')) from dual;
select replace('看i给客人 是 二万人','看i给客人','g.pnhl【哦哦') from dual;
select trim('蛤' from '蛤蟆蛤么蛤嘛') from dual;
select substr('零五七九二哦i觉得萨芬离开你的萨拉九分',3,8) 截取字符串,length(substr('零五七九二哦i觉得萨芬离开你的萨拉九分',3,8)) 字符串长度,lengthb(substr('零五七九二哦i觉得萨芬离开你的萨拉九分',3,8)) 字节长度  from dual;
select abs(100),abs(-100) from dual;
select instr('拉萨扩大解方放 期望对方','方',1,2) instring from dual;
select floor(3.1415927),floor(-3.1415927) from dual;
select ceil(3.1415927),ceil(-3.1415927) from dual;
SELECT ROUND (55.655, 2), ROUND (55.654, 2), ROUND (45.654, -1), ROUND (45.654, -2), ROUND (55.654, -2) FROM   DUAL;
select to_char(add_months(to_date('199912','yyyymm'),2),'yyyymm') from dual;
select to_char(add_months(to_date('199912','yyyymm'),-2),'yyyymm') from dual;
Select HEXTORAW ('AB56') 中 from dual;
select to_number('1999') year from dual;
select nvl('撒赖打开附件里的萨克','啊撒旦范德萨分') from dual;
select nvl('','啊撒旦范德萨分') from dual;
