--echo  ##全量函数、表达式gb18030-5//gb18030-5-1

--disable_warnings
--error 0,942
drop table gbktable CASCADE CONSTRAINTS purge;
--enable_warnings

create table gbktable
(
c0 int,
c1 nvarchar2(200),
c2 varchar2(20),
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 char(200),
c8 raw(200),
c9 number(10,3),
CONSTRAINT PK_test2 PRIMARY KEY (c1,c2) 
);

insert into gbktable values(1,'中文测试001','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.2);
insert into gbktable values(2,'中文测试002','中文测试002','aaabbb','中文测试002',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'c','ee',100);
insert into gbktable values(3,'中文测试003','中文测试001','aaabbb','中文测试003',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'d','ff',100.3);
insert into gbktable values(4,'aaabbb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','cc',200);
insert into gbktable values(5,'a','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.22);
insert into gbktable values(6,'bb','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',3000);
insert into gbktable values(7,'20060604','20060604','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',2000.33);
insert into gbktable values(8,'2006-06-04','aaabbb','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',1000);
insert into gbktable values(9,'中文测试009','aaabbb','200','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','200',10000);
insert into gbktable values(10,'中文测试010','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','300',999999);
insert into gbktable values(11,'中文测试011','aaabbb','300','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd  hh24:mi:ss'),'a','300',999999);

###OB全量支持的表达式TestExprFactory.oracle_expr_names
###sys_connect_by_path
###select sys_connect_by_path(c1,'>') tree from gbktable start with c1='King' connect by prior c2 = '中文测试001';
###select sys_connect_by_path(c1,'=') tree from gbktable start with c1='King' connect by prior c2 = '中文测试001';

--disable_warnings
--error 0,942
drop table emp CASCADE CONSTRAINTS purge;
--enable_warnings
create table emp (empno nvarchar2(5), ename varchar2(8), mgr varchar2(8),CONSTRAINT PK_test3 PRIMARY KEY (empno,ename));
insert into emp (EMPNO, ENAME, MGR)
values ('1', 'jim', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('2', 'tom', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('3', 'tim', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('4', 'lily', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('5', 'mary', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('6', 'tid', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('7', 'King', '10');
insert into emp (EMPNO, ENAME, MGR)
values ('8', 'kenvin', '10');
insert into emp (EMPNO, ENAME, MGR)
values ('9', 'shema', '8');
insert into emp (EMPNO, ENAME, MGR)
values ('10', 'john', '0');
insert into emp (EMPNO, ENAME, MGR)
values ('0', 'root', '');
###SELECT * FROM emp;
select sys_connect_by_path(ename,'>') tree from emp start with ename='King' connect by prior empno = mgr;
select sys_connect_by_path(ename,'/') tree from emp start with ename='King' connect by prior empno = mgr;
###OB结果与ORACLE不一致；
###select sys_connect_by_path(ename,'/') tree,level from emp start with ename='King' connect by prior mgr= empno;

###timestamp_nvl，ORACLE不支持

###to_date函数验证
select to_date('2004-05-07 13:23:44','yyyy-mm-dd hh24:mi:ss') from dual;
select to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') from gbktable t where t.c0 = 7;

select to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') from gbktable t where t.c0 = 7;
select to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss') from dual;
###29685077- 【表达式bug】OB oracle模式to_date日期之后的数据直接比较，ORACLE可以比较无报错，OB有异常报错OBE-01858。
###select * from gbktable t where t.c0 = 7 and to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') = to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss');

select * from dual where to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss') = to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss');
select * from dual where to_date('20060604','yyyy-mm-dd hh24:mi:ss') = to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss');

###select t.c1,to_date(t.c1,'yyyy-mm-dd hh24:mi:ss'),to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss'),to_date('20060604','yyyy-mm-dd hh24:mi:ss') from gbktable t where t.c0 = 7;
###create table testdate as select t.c1 as c1,to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') as c2,to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss') as c3,to_date('20060604','yyyy-mm-dd hh24:mi:ss')  as c4 from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') = to_date(t.c1,'yyyy-mm-dd hh24:mi:ss');
###select * from gbktable t where t.c0 = 7 and to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') = to_date('2006-06-04','yyyy-mm-dd hh24:mi:ss');
###select * from gbktable t where t.c0 = 7 and to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') = to_date('20060604','yyyy-mm-dd hh24:mi:ss');

--disable_result_log
select floor(sysdate - to_date('20020405','yyyymmdd')) from dual;    
select months_between(to_date('02-01-1999','MM-DD-YYYY'),to_date('12-31-1998','MM-DD-YYYY')) "MONTHS" FROM DUAL; 
--enable_result_log

###union使用;时间为null的用法
select t.c0, to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') from gbktable t where t.c0 = 7
UNION
select 1, TO_DATE(null) from dual;

##先to date再to char
select t.c1 from gbktable t where t.c0 = 7 and to_date(t.c1,'yyyy-mm-dd hh24:mi:ss') between to_date('20011201','yyyy-mm-dd hh24:mi:ss') and to_date('20201231','yyyy-mm-dd hh24:mi:ss');
select to_char(to_date('2002-08-26','yyyy-mm-dd'),'day','NLS_DATE_LANGUAGE = American') from dual; 
select * from dual where trim(to_char(to_date('2002-08-26','yyyy-mm-dd'),'day','NLS_DATE_LANGUAGE = American')) = 'monday'; 

#####查找2002-02-28至2002-02-01间除星期一和七的天数
##select count(*);
##from (select rownum-1 rnum
##from all_objects
##where rownum <= to_date('2002-02-28','yyyy-mm-dd') - to_date('2002-
##02-01','yyyy-mm-dd')+1
##)where to_char( to_date('2002-02-01','yyyy-mm-dd')+rnum-1, 'D')
##not in ( '1', '7' );

##查找月份	
select months_between(to_date('01-31-1999','MM-DD-YYYY'),to_date('12-31-1998','MM-DD-YYYY')) "MONTHS" FROM DUAL; 

##年月日的处理
select older_date,newer_date,years,months,abs(trunc(newer_date-add_months(older_date,years*12+months))) days from 
(select trunc(months_between( newer_date, older_date )/12) YEARS,
mod(trunc(months_between( newer_date, older_date )),12 ) MONTHS,
newer_date,
older_date
from (
select t.c1 older_date, add_months(t.c1,rownum)+rownum newer_date
from gbktable t where t.c0 =7
)
);

###to_char
--disable_result_log
select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as nowTime from dual;
select to_char(sysdate,'yyyy') as nowYear   from dual;
select to_char(sysdate,'mm')    as nowMonth from dual;
select to_char(sysdate,'dd')    as nowDay    from dual;
select to_char(sysdate,'hh24') as nowHour   from dual;
select to_char(sysdate,'mi')    as nowMinute from dual;
select to_char(sysdate,'ss')    as nowSecond from dual;

###select to_char( to_date(222,'J'),'Jsp') from dual;
 
select to_char(to_date('2002-08-26','yyyy-mm-dd'),'day') from dual;     
select to_char(to_date('2002-08-26','yyyy-mm-dd'),'day','NLS_DATE_LANGUAGE = American') from dual;     
###select to_char(sysdate,'hh:mi:ss') TIME from all_objects;
select sysdate ,to_char(sysdate,'hh') from dual;
select sysdate ,to_char(sysdate,'hh24') from dual;  
select to_char(add_months(last_day(sysdate) +1, -2), 'yyyymmdd'),last_day(sysdate) from dual;
###找出今年的天数
select add_months(trunc(sysdate,'year'), 12) - trunc(sysdate,'year') from dual;
select to_char(last_day(to_date('022020','mmyyyy')),'dd') from dual;
select TO_DATE(FLOOR(TO_CHAR(sysdate,'SSSSS')/300) * 300,'SSSSS') ,TO_CHAR(sysdate,'SSSSS') from dual;
select TO_CHAR(SYSDATE,'DDD'),sysdate from dual;

select Days,
A,
TRUNC(A*24) Hours,
TRUNC(A*24*60 - 60*TRUNC(A*24)) Minutes,
TRUNC(A*24*60*60 - 60*TRUNC(A*24*60)) Seconds,
TRUNC(A*24*60*60*100 - 100*TRUNC(A*24*60*60)) mSeconds
from
(
select
trunc(sysdate) Days,
sysdate - trunc(sysdate) A
from dual
);
select TRUNC(0.48733*24*60*60*100 - 100*TRUNC(0.48733*24*60*60)) from dual;
select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),to_char(sysdate+3*365,'yyyy-mm-dd hh24:mi:ss') as newTime from dual;
select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),add_months(sysdate,3) as newTime from dual;
select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),to_char(sysdate+4,'yyyy-mm-dd hh24:mi:ss') as newTime from dual;
select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),to_char(sysdate+4/24,'yyyy-mm-dd hh24:mi:ss') as newTime from dual;
select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),to_char(sysdate+5/24/60,'yyyy-mm-dd hh24:mi:ss') as newTime from dual;
select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),to_char(sysdate+3/24/60/60,'yyyy-mm-dd hh24:mi:ss') as newTime from dual;
select to_char(add_months(trunc(sysdate,'year'),Rownum - 1),'yyyy-mm') from dual Connect By Rownum <= 12;

--enable_result_log 

###to_clob：29692409- 【oracle模式】【GBK字符集】OB oracle模式to_clob处理多种字段类型，均有中文乱码
select to_clob(t.c1) from gbktable t;
select to_clob(t.c2) from gbktable t;
--error 00932
select to_clob(t.c3) from gbktable t;
select to_clob(t.c4) from gbktable t;
select to_clob(t.c8) from gbktable t;
select to_clob(t.c9) from gbktable t;

###to_timestamp

###TO_TIMESTAMP 和 TO_TIMESTAMP_TZ处理日期数据有报错，ORACLE正常。
SELECT TO_TIMESTAMP ('10-Sep-02 14:10:10.123000', 'DD-Mon-RR HH24:MI:SS.FF') FROM DUAL;
SELECT TO_TIMESTAMP ('04-Jun-06', 'DD-Mon-RR HH24:MI:SS.FF') FROM DUAL;
###SELECT * FROM gbktable t where t.c0 = 7 and TO_TIMESTAMP ('04-Jun-06', 'DD-Mon-RR HH24:MI:SS.FF') = '2006-06-04 00:00:00.000000000';

##SELECT TO_TIMESTAMP (substr(t.c6,1,10), 'DD-Mon-RR HH24:MI:SS') FROM gbktable t where t.c0 = 11;

###to_timestamp_tz

SELECT TO_TIMESTAMP_TZ ('10-Sep-02 14:10:10.123000', 'DD-Mon-RR HH24:MI:SS.FF') FROM DUAL;
##SELECT TO_TIMESTAMP_TZ (substr(t.c6,1,10), 'DD-Mon-RR HH24:MI:SS') FROM gbktable t where t.c0 = 11;


###sysdate

--disable_result_log
select sysdate from dual;
--enable_result_log 

###partition_hash_v2 --ORACLE无对比项
###partition_hash --ORACLE无对比项
--disable_warnings
--error 0,942
drop table t_partition_hash CASCADE CONSTRAINTS purge;
--enable_warnings

create table t_partition_hash(id nvarchar2(20),name varchar2(20),CONSTRAINT PK_test55 PRIMARY KEY (id,name))
partition by hash(id)(
partition t_分区一,
partition t_分区二,
partition t_分区三);
insert into t_partition_hash values ('第一','第one');
insert into t_partition_hash values ('第二','第two');
insert into t_partition_hash values ('第三','第three');
select partition_name,high_value,tablespace_name from user_tab_partitions where table_name='T_PARTITION_HASH';
select * from t_partition_hash;
select * from t_partition_hash t where t.id = '第一' and t.name = '第one';

###ora_decode --ORACLE无对比项
###decode 验证

select decode(t.c1,'中文测试001','好的','aaabbb','不好','20060604','再说') from gbktable t;
select * from gbktable t where decode(t.c1,'中文测试001','好的','aaabbb','不好','20060604','再说') = '好的';
select decode(t.c2,'中文测试001','好的','aaabbb','不好','20060604','再说') from gbktable t;
select * from gbktable t where decode(t.c2,'中文测试001','好的','aaabbb','不好','20060604','再说') = '好的';

###uid???实现方法不一致？

####SELECT dbu.username, dbu.user_id FROM dba_users dbu WHERE dbu.username = (SELECT User FROM DUAL);
####SELECT dbu.username, dbu.userid FROM dba_users dbu WHERE dbu.username = (SELECT User FROM DUAL);
####SELECT uid FROM dual;
###SELECT dbu.username, dbu.userid FROM dba_users dbu WHERE dbu.userid = '1100611139404778';

###replace

select replace(t.c1,'中文测试00','好的好的') from gbktable t;
select * from gbktable t where replace(t.c1,'中文测试00','好的好的') = '好的好的1';
select replace(t.c2,'中文测试00','好的好的') from gbktable t;
select * from gbktable t where replace(t.c2,'中文测试00','好的好的') = '好的好的1';

###translate
select translate('abcdefga','abc','wo') 返回值 from dual;
select translate('中文测试001','中文','英文') 返回值 from dual;
select translate(t.c1,'中文','英文') 返回值 from gbktable t;
select count(*) from gbktable t where translate(t.c1,'中文','英文') = '英文测试010';
select translate(t.c2,'中文','英文') 返回值 from gbktable t;
select count(*) from gbktable t where translate(t.c2,'中文','英文') = '英文测试001';

###length
select '返回值' from DUAL;
select '返回值' AS 返回值 from DUAL;

select length(t.c1) 返回值 from gbktable t;
select * from gbktable t where length(t.c1) = 7;

###lengthb
select lengthb(t.c1) 返回值 from gbktable t;
select * from gbktable t where lengthb(t.c1) = 14;

###effective_tenant_id --ORACLE无对比项

###rownum
select rownum,c1 from gbktable where rownum < 10;
select rownum,c1 from gbktable where rownum != 9;

###trunc
--disable_result_log
select sysdate S1,     
trunc(sysdate) S2,
trunc(sysdate,'year') YEAR,
trunc(sysdate,'month') MONTH,
trunc(sysdate,'day') DAY
from dual;
   
SELECT Trunc(Trunc(SYSDATE, 'MONTH') - 1, 'MONTH') First_Day_Last_Month,
Trunc(SYSDATE, 'MONTH') - 1 / 86400 Last_Day_Last_Month,
Trunc(SYSDATE, 'MONTH') First_Day_Cur_Month,
LAST_DAY(Trunc(SYSDATE, 'MONTH')) + 1 - 1 / 86400 Last_Day_Cur_Month
FROM dual;
--enable_result_log 

###user --ORACLE无对比项
###cast

select cast(c1 as varchar2(8)) as 新列 from gbktable;
select cast(c2 as varchar2(8)) as 新列 from gbktable;
select cast(c1 as char(8)) as 新列 from gbktable;
select cast(c2 as char(8)) as 新列 from gbktable;

###%

select t.c1 from gbktable t where t.c1 like '中文%' and t.c2 like '中文%';
select t.c1 from gbktable t where t.c1 not like '中文%' and t.c2 not like '中文%';

###remainder
select remainder(3.5,2) from dual;

select remainder(t.c1,2) from gbktable t where t.c0 = 7;
##select * from gbktable t where t.c0 = 7 and remainder(t.c1,2) = 0;

###abs

select abs(20060604) from dual;
select abs(t.c1) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and abs(t.c1) = '20060604';

###concat

select concat(t.c1,t.c2) from gbktable t;
select * from gbktable t where concat(t.c1,t.c2) = '中文测试001中文测试001' ;
select * from gbktable t where concat(t.c1,t.c2) = '中文测试001中文测试001' and concat(t.c1,t.c2) = concat(t.c2,t.c1);

select concat(concat(t.c1,t.c2),t.c7) from gbktable t;
###select * from gbktable t where trim(concat(concat(t.c1,t.c2),t.c7)) = '中文测试001中文测试001a';

###to_number

select to_number(t.c1) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and to_number(t.c1) = 20060604;
###select * from gbktable t where t.c0 = 7 and to_number(t.c7) = 200;

###rpad

SELECT '甲骨文' ORA_STR,
LENGTHB('甲骨文') ORA_STR_LENGTH,
RPAD('甲骨文', 10, '$') ORA_RPAD_STR,
LENGTHB(RPAD('甲骨文', 10, '$')) ORA_RPAD_STR_LENGTH
FROM dual;

SELECT t.c1 ORA_STR,
LENGTHB(t.c1) ORA_STR_LENGTH,
RPAD(t.c1, 30, '$') ORA_RPAD_STR,
LENGTHB(RPAD(t.c1, 20, '$')) ORA_RPAD_STR_LENGTH
FROM gbktable t where t.c1 = '中文测试001' and RPAD(t.c1, 10, '$') = '中文测试00';

###lpad
SELECT t.c1 ORA_STR,
LENGTHB(t.c1) ORA_STR_LENGTH,
LPAD(t.c1, 30, '$') ORA_RPAD_STR,
LENGTHB(LPAD(t.c1, 20, '$')) ORA_RPAD_STR_LENGTH
FROM gbktable t where t.c1 = '中文测试001' and LPAD(t.c1, 10, '$') = '中文测试00';

###instr
select instr(t.c1,'文测') from gbktable t;
select * from gbktable t where instr(t.c1,'文测') = 2;
select instr(t.c1,'文测',1,1) from gbktable t;
select * from gbktable t where instr(t.c1,'文测',1,1) = 2;

###instrb
select instrb(t.c1,'文测') from gbktable t;
select * from gbktable t where instrb(t.c1,'文测') = 3;
select instrb(t.c1,'文测',1,1) from gbktable t;
select * from gbktable t where instrb(t.c1,'文测',1,1) = 3;


###substr
select substr(t.c1,3,5) from gbktable t;
select * from gbktable t where substr(t.c1,3,5) = '测试001';

###substrb
select substrb(t.c1,3,5) from gbktable t;
select * from gbktable t where substrb(t.c1,3,5) = '文测';

###initcap

select initcap('hello world') from dual;
select initcap('汉ello world,汉elloworld,hello汉world,hello 汉world') from dual;
select initcap(t.c1) from gbktable t;
select * from gbktable t where initcap(t.c1) = '中文测试001';
select initcap(t.c2) from gbktable t;
select * from gbktable t where initcap(t.c2) = '中文测试001';

###round
--disable_result_log
select sysdate S1,
round(sysdate) S2 ,
round(sysdate,'year') YEAR,
round(sysdate,'month') MONTH ,
round(sysdate,'day') DAY from dual;
--enable_result_log 

###coalesce

SELECT COALESCE(NULL,NULL,3,4,5) FROM dual;
SELECT COALESCE(NULL,NULL,t.c1) FROM gbktable t;
SELECT COALESCE(NULL,NULL,cast(t.c2 as nchar(10))) FROM gbktable t;

SELECT * FROM gbktable t where COALESCE(NULL,NULL,t.c1) = '?????';

SELECT COALESCE(NULL,NULL,t.c2) FROM gbktable t;
SELECT COALESCE(NULL,NULL,t.c7) FROM gbktable t;

###USERENV：
#####1.ISDBA 查看当前用户是否是DBA如果是则返回true
###select userenv('isdba') from dual;
######2.SESSION 返回会话标志
###select userenv('sessionid') from dual;
###select * from dual where userenv('sessionid') = '3221631142';
######4.ENTRYID 返回会话人口标志
###select userenv('entryid') from dual;
######5.INSTANCE 返回当前INSTANCE的标志
###select userenv('instance') from dual;
######6.LANGUAGE 返回当前环境变量
###select userenv('language') from dual;
######7.LANG 返回当前环境的语言的缩写
###select userenv('lang') from dual;
######8.TERMINAL 返回用户的终端或机器的标志
###select userenv('terminal') from dual;
######9.VSIZE(X) 返回X的大小(字节)数
###select vsize(user),user from dual;

###SYS_CONTEXT
###OB不支持
###select
###　　SYS_CONTEXT('USERENV','TERMINAL') terminal,
###　　SYS_CONTEXT('USERENV','LANGUAGE') language,
###　　SYS_CONTEXT('USERENV','SESSIONID') sessionid,
###　　SYS_CONTEXT('USERENV','INSTANCE') instance,
###　　SYS_CONTEXT('USERENV','ENTRYID') entryid,
###　　SYS_CONTEXT('USERENV','ISDBA') isdba,
###　　SYS_CONTEXT('USERENV','NLS_TERRITORY') nls_territory,
###　　SYS_CONTEXT('USERENV','NLS_CURRENCY') nls_currency,
###　　SYS_CONTEXT('USERENV','NLS_CALENDAR') nls_calendar,
###　　SYS_CONTEXT('USERENV','NLS_DATE_FORMAT') nls_date_format,
###　　SYS_CONTEXT('USERENV','NLS_DATE_LANGUAGE') nls_date_language,
###　　SYS_CONTEXT('USERENV','NLS_SORT') nls_sort,
###　　SYS_CONTEXT('USERENV','CURRENT_USER') current_user,
###　　SYS_CONTEXT('USERENV','CURRENT_USERID') current_userid,
###　　SYS_CONTEXT('USERENV','SESSION_USER') session_user,
###　　SYS_CONTEXT('USERENV','SESSION_USERID') session_userid,
###　　SYS_CONTEXT('USERENV','PROXY_USER') proxy_user,
###　　SYS_CONTEXT('USERENV','PROXY_USERID') proxy_userid,
###　　SYS_CONTEXT('USERENV','DB_DOMAIN') db_domain,
###　　SYS_CONTEXT('USERENV','DB_NAME') db_name,
###　　SYS_CONTEXT('USERENV','HOST') host,
###　　SYS_CONTEXT('USERENV','OS_USER') os_user,
###　　SYS_CONTEXT('USERENV','EXTERNAL_NAME') external_name,
###　　SYS_CONTEXT('USERENV','IP_ADDRESS') ip_address,
###　　SYS_CONTEXT('USERENV','NETWORK_PROTOCOL') network_protocol,
###　　SYS_CONTEXT('USERENV','BG_JOB_ID') bg_job_id,
###　　SYS_CONTEXT('USERENV','FG_JOB_ID') fg_job_id,
###　　SYS_CONTEXT('USERENV','AUTHENTICATION_TYPE') authentication_type,
###　　SYS_CONTEXT('USERENV','AUTHENTICATION_DATA') authentication_data
###　　from dual;
###select SYS_CONTEXT('USERENV','TERMINAL') terminal from dual;

###lower
select lower(t.c1) from gbktable t;
select * from gbktable t where lower(t.c1) = '中文测试001';
select lower(t.c2) from gbktable t;
select * from gbktable t where lower(t.c2) = '中文测试001';

###upper
select upper(t.c1) from gbktable t;
select * from gbktable t where upper(t.c1) = '中文测试001';
select upper(t.c2) from gbktable t;
select * from gbktable t where upper(t.c2) = '中文测试001';

###trim
###ltrim
###rtrim

SELECT TRIM('         中文        测试 ') FROM dual;
select length(TRIM('       中文           测试 ')) FROM dual;
SELECT LTRIM('       中文        测试         ') FROM dual;
select length(LTRIM('       中文           测试 ')) FROM dual;
SELECT RTRIM('       中文          测试       ') FROM dual;


SELECT TRIM(t.c1) FROM gbktable t;
SELECT TRIM(t.c2) FROM gbktable t;
SELECT TRIM(cast(t.c2 as nchar(10))) FROM gbktable t;

SELECT LTRIM(t.c1) FROM gbktable t;
SELECT RTRIM(t.c1) FROM gbktable t;

SELECT trim(leading 'd' from 'dfssa') FROM dual;
SELECT trim(both '1' from '123sfd111') FROM dual;
SELECT trim(trailing '2' from '213dsq12') FROM dual;

SELECT trim(leading '中' from '中文测试1') FROM dual;
SELECT trim(leading '中' from t.c1) FROM gbktable t;
SELECT * FROM gbktable t where trim(leading '中' from t.c1) = 'aaabbb';

SELECT trim(both '1' from '123sfd111') FROM dual;
SELECT trim(both '中' from t.c1) FROM gbktable t;
SELECT * FROM gbktable t where trim(both '中' from t.c1) = 'aaabbb';

SELECT trim(trailing '2' from '213dsq12') FROM dual;
SELECT trim(trailing '中' from t.c1) FROM gbktable t;
SELECT * FROM gbktable t where trim(trailing '中' from t.c1) = 'aaabbb';

--error 600,30001
SELECT trim(trailing '12' from '123dsq12') FROM dual;

SELECT rtrim('aaaaminb','main') FROM dual;
SELECT rtrim('aaaaminb','mainb') FROM dual;
SELECT ltrim('ccbcminb','cb') FROM dual;

SELECT rtrim('中文测试001','测试0011') FROM gbktable t;
SELECT rtrim('中文测试001','测试00') FROM gbktable t;
SELECT ltrim('中文测试001','哈哈中文') FROM gbktable t;

###sinh
select sinh(t.c1 - 20060603) from gbktable t where t.c0 = 7;
select sinh(t.c2 - 20060603) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and sinh(t.c2 - 20060603) = '1.17520119364380145688238185059560081516';
--error 00932
select sinh(t.c8-99) from gbktable t where t.c0 = 4;
select sinh(t.c9) from gbktable t where t.c0 = 5;


###cosh

select cosh(t.c1 - 20060603) from gbktable t where t.c0 = 7;
select cosh(t.c2 - 20060603) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and cosh(t.c2 - 20060603) = '1.1.5430806348152437784779056207570616826';
--error 00932
select cosh(t.c8-99) from gbktable t where t.c0 = 4;
select cosh(t.c9) from gbktable t where t.c0 = 5;


###tanh

select sinh(t.c1 - 20060603) from gbktable t where t.c0 = 7;
select sinh(t.c2 - 20060603) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and sinh(t.c2 - 20060603) = 1.17520119364380145688238185059560081516;
--error 00932
select sinh(t.c8-99) from gbktable t where t.c0 = 4;
select sinh(t.c9) from gbktable t where t.c0 = 5;

###nvl

select nvl(null,t.c1) from gbktable t;
select nvl(null,t.c2) from gbktable t;
select nvl(t.c1,null) from gbktable t;
select nvl(t.c2,null) from gbktable t;
select * from gbktable t where nvl(null,t.c1) = nvl(null,t.c2) and nvl(null,t.c1) = '中文测试001';
select count(*) from gbktable t where nvl(null,t.c1) = nvl(t.c1,null) ;


###ceil

--disable_result_log
select ceil(t.c1 - 20060603.5) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and ceil(t.c1 - 20060603.5) = 1;
select ceil(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))/365) as spanYears from dual;
select ceil(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))) as spanDays from dual;
select ceil(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))*24) as spanHours from dual;
select ceil(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))*24*60) as spanMinutes from dual;
select ceil(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))*24*60*60) as spanSeconds from dual;
	  
--enable_result_log 


###floor
--disable_result_log
select floor(t.c1 - 20060603.5) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and floor(t.c1 - 20060603.5) = 0;
select floor(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))/365) as spanYears from dual;
select floor(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))) as spanDays from dual;
select floor(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))*24) as spanHours from dual;
select floor(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))*24*60) as spanMinutes from dual;
select floor(to_number(sysdate-to_date('2007-11-02 15:55:03','yyyy-mm-dd hh24:mi:ss'))*24*60*60) as spanSeconds from dual;
	  
--enable_result_log
###asin

select asin(t.c1 - 20060603) from gbktable t where t.c0 = 7;
select asin(t.c2 - 20060603) from gbktable t where t.c0 = 7;
###select * from gbktable t where t.c0 = 7 and asin(t.c2 - 20060603) = '1.5707963267948966192313216916397514421';
--error 00932
select asin(t.c8-99) from gbktable t where t.c0 = 4;


###数学函数同上，与字符集无关
###acos
###atan
###atan2
###sqrt

###sign
select sign(t.c1 - 20060603) from gbktable t where t.c0 = 7;
select sign(t.c1 - 20060604) from gbktable t where t.c0 = 7;
select sign(t.c1 - 20060605) from gbktable t where t.c0 = 7;

###systimestamp
--disable_result_log
select systimestamp from dual;
select to_char(systimestamp,'SSSS.FF') from dual;
select to_char(systimestamp,'YYYY-MM-DD HH24:MI:SS.FF9') from dual;
--enable_result_log

###localtimestamp
--disable_result_log
select localtimestamp from dual;
select to_char(localtimestamp,'SSSS.FF') from dual;
select to_char(localtimestamp,'YYYY-MM-DD HH24:MI:SS.FF9') from dual;
--enable_result_log

###set_collation --ORACLE无对比项
###width_bucket

###OB结果与oracle不一致
select empno,
sum(mgr) as t_sal,
width_bucket(sum(mgr), 0, 10, 6) as t_bucket
from emp t
group by t.empno
order by t.empno
;

###chr
###extract
--disable_result_log
select to_char(trunc(sysdate,'month') + Rownum - 1,'yyyy-mm-dd')
from dual Connect By Rownum <= extract(Day From last_day(trunc(Sysdate,'month')));

SELECT EXTRACT(YEAR FROM SYSDATE) FROM DUAL;
SELECT EXTRACT(MONTH FROM SYSDATE) FROM DUAL;
SELECT EXTRACT(DAY FROM SYSDATE) FROM DUAL;

--enable_result_log 

select CHR(116) from dual;
select CHR(ascii(t.c1)) from gbktable t where t.c0=4;
select ascii(t.c1) from gbktable t where t.c0=4;
select * from gbktable t where t.c0=4 and CHR(ascii(t.c1)) = 'a';

select ascii(t.c1) from gbktable t where t.c0=1;
select * from gbktable t where t.c0=1 and CHR(ascii(t.c1)) = 'N-';

###+
###=
###<=
###<
###>
###<=>
###>=

select t.c1,t.c2 from gbktable t where t.c1 <> t.c2;
select t.c1,t.c2 from gbktable t where t.c1 >= t.c2;

###--ORACLE无对比项
###neg
###ref_query
###subquery_equal
###subquery_not_equal
###subquery_null_safe_equal
###subquery_greater_equal
###subquery_greater_than
###subquery_less_equal
###subquery_less_than
###remove_const


###is
###is_not
select t.c1,t.c2 from gbktable t where t.c1 is NULL;
select t.c1,t.c2 from gbktable t where t.c1 is NOT NULL;

###between
###not_between

select t.c1,t.c2 from gbktable t where t.c1 between '中文测试001' and '中文测试003';
select t.c1,t.c2 from gbktable t where t.c1 not between '中文测试001' and '中文测试003';

###like
select t.c1,t.c2 from gbktable t where t.c1 like '中文测试00%';

###已有问题单
###regexp_substr
###regexp_instr
###regexp_replace

###regexp_count
select REGEXP_COUNT('GEORGE','GE',1,'i') from DUAL;
select REGEXP_COUNT(t.c1,'中国',1,'i') from gbktable t;

###regexp_like

select * from (
select 1 id, 'ABC' s1 from dual union all
select 2 id,'AC' s1 from dual union all
select 3 id,'BC' s1 from dual union all
select 4 id,null s1 from dual union all
select 5 id,'' s1 from dual union all
select 6 id,'B' s1 from dual union all
select 7 id,'D' s1 from dual
) where regexp_like(nvl(s1,' '),'A|C')
;

select * from (
select 1 id, 'ABC' s1 from dual union all
select 2 id,'AC' s1 from dual union all
select 3 id,'BC' s1 from dual union all
select 4 id,null s1 from dual union all
select 5 id,'' s1 from dual union all
select 6 id,'B' s1 from dual union all
select 7 id,'D' s1 from dual
) where not regexp_like(nvl(s1,' '),'A|C')
;

select * from gbktable t where regexp_like(t.c1,'^[0-9]|[-]$');

###!
select * from gbktable t where t.c2 != '中文测试001';
select * from gbktable t where t.c1 != '中文测试001';

###&&  -- &&用来创建一个持久变量，当用&&命令引用这个变量时，不会每次遇到该变量就提示用户键入值，而只是在第一次遇到时提示一次
###||
select t.c1||t.c2 from gbktable t;
select * from gbktable t where t.c1||t.c2 = '中文测试001中文测试001';

###in
###not_in

select * from gbktable t where t.c1 in ('中文测试001','中文测试002','中文测试003','aaabbb','20060604','2006-06-04','中文测试009');
select * from gbktable t where t.c1 not in ('中文测试001','中文测试002','中文测试003','aaabbb','20060604','2006-06-04','中文测试009');
select * from gbktable t where t.c2 in ('中文测试001','中文测试002','中文测试003','aaabbb','20060604','2006-06-04','中文测试009');
select * from gbktable t where t.c2 not in ('中文测试001','中文测试002','中文测试003','aaabbb','20060604','2006-06-04','中文测试009');

###arg_case
###case

select case when t.c1 between '中文测试001' and '中文测试001' then 'low好的'
            when t.c1 between '中文测试002' and '中文测试008' then 'mid不好'
            when t.c1 between '中文测试008' and '中文测试009' then 'highnice啊'
            else 'unknow'
       end
from gbktable t;


###quote
select q'[My Name is "Wang Xiu Li"]' names from dual;
select q'[中文英文web-！@#￥%]' names from dual;

###conv --ORACLE无对比项

###OB语法暂不支持
###SELECT CONVERT('? ê í ? ? A B C D E ', 'US7ASCII', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'UTF8', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'AL32UTF8', 'WE8ISO8859P1') FROM DUAL; 
###SELECT CONVERT('? ê í ? ? A B C D E ', 'AL32UTF8', 'UTF8') FROM DUAL; 


###assign --ORACLE无对比项
###get_user_var
###get_sys_var
###agg_param_list
###obj_access
###connect_by_root
###get_package_var
###get_subprogram_var
###shadow_uk_project
###^
###nextval
###column_conv
###values
###part_id
###seq_value
###to_type
###!=
###-
###position
###*
###/
###lnnvl
###cur_date
###pad --ORACLE无对比项

###exists
###not exists

select * from gbktable where exists (select * from gbktable where c1 like '%中文%');
select * from gbktable where not exists (select * from gbktable where c1 like '%中文%');

###current_timestamp
##select current_timestamp from dual;

###default
###
###--disable_warnings
###--error 0,942
###drop table gbktable7 CASCADE CONSTRAINTS purge;
###--enable_warnings
###
###create table gbktable7(c1 nchar(20) default '中文测试001',c2 nvarchar2(20) default '中文测试002',c3 varchar2(20) default '中文测试003');
###insert into gbktable7 (c3) values (default);
###select * from gbktable7;
###select * from gbktable7 t where t.c1 = '中文测试001';
###select * from gbktable7 t where t.c2 = '中文测试002';
###select * from gbktable7 t where t.c3 = '中文测试003';
###
###--disable_warnings
###--error 0,942
###drop table gbktable7 CASCADE CONSTRAINTS purge;
###--enable_warnings

###user_define_function
###nvl2

select nvl2(t.c1,t.c1,t.c2) from gbktable t ;
select nvl2(NULL,t.c1,t.c2) from gbktable t ;
select nvl2(t.c2,t.c1,t.c2) from gbktable t ;

select * from gbktable t where nvl2(t.c1,t.c1,t.c2) = '中文测试001';


###TO_BINARY_FLOAT
###TO_BINARY_DOUBLE

###SELECT TO_BINARY_FLOAT(1222.111) from dual;
###--error 1722
###SELECT TO_BINARY_FLOAT('中文测试001') from dual;
###
###SELECT TO_BINARY_FLOAT(t.c1) from gbktable t where t.c0 = 7;
###SELECT TO_BINARY_DOUBLE(t.c1) from gbktable t where t.c0 = 7;

###nullif
###
###select nullif(t.c1,t.c2) from gbktable t;
###select nullif(t.c1,'中文测试001') from gbktable t;
###select nullif(t.c2,'中文测试001') from gbktable t;

###stmt_id --ORACLE无对比项
###spm_load_plans_from_plan_cache
###spm_alter_baseline
###spm_drop_baseline --ORACLE无对比项


###least
###greatest
###select greatest('01-1月-04','04-1月-04','10-2月-04') from dual
###select greatest (1, 3, 2 ) from dual ;
###select greatest ( 'A', 'B', 'C' ) from dual ;
###select greatest (null, 'B', 'C' ) from dual ;
###
######
######select greatest (t.c1, t.c2,t.c3,t.c4,t.c5 ) from  gbktable t ;
######select greatest (t.c1, t.c2 ) from  gbktable t ;
######select greatest (t.c2,t.c3 ) from  gbktable t ;
######select greatest (t.c2,t.c4 ) from  gbktable t ;
######select greatest (t.c2,t.c5 ) from  gbktable t ;
###select greatest (t.c5, t.c6 ) from  gbktable t ;
######select greatest (t.c7,t.c8) from  gbktable t ;
###select greatest (t.c7, t.c9 ) from  gbktable t ;
###
### 
###select least (1, 3, 2 ) from dual ;
###select least ( 'A', 'B', 'C' ) from dual ;
###select least (null, 'B', 'C') from dual ;
###select least (t.empno, t.mgr ) from  scott.emp t ;
###
######
######select least (t.c1, t.c2,t.c3,t.c4,t.c5 ) from  gbktable t ;
######select least (t.c1, t.c2 ) from  gbktable t ;
######select least (t.c2,t.c3 ) from  gbktable t ;
######select least (t.c2,t.c4 ) from  gbktable t ;
######select least (t.c2,t.c5 ) from  gbktable t ;
###select least (t.c5, t.c6 ) from  gbktable t ;
######select least (t.c7,t.c8) from  gbktable t ;
###select least (t.c7, t.c9 ) from  gbktable t ;
###
###host_ip --ORACLE无对比项
###rpc_port
###is_serving_tenant
###BITAND

###select t.* from gbktable t where t.c0 = 7 and bitand(to_number(t.c1), 20)>0;
###select t.*  from gbktable t where t.c0 = 7 and bitand(to_number(t.c1), 10)= to_number(t.c1);
  
###hextoraw
###rawtohex:29721829- 【ORACLE模式】【表达式bug】OB oracle模式nvarchar2字段类型作为hextoraw/rawtohex函数的的参数时，OB不识别报错OBE-01465: invalid hex number，ORACLE正常识别

###select hextoraw('abcdef') from dual;
###select hextoraw(t.c1) from gbktable t where t.c0 =7;
###select hextoraw(t.c2) from gbktable t where t.c0 =7;
###
###select rawtohex(hextoraw(t.c1)) from gbktable t where t.c0 =7;
###select rawtohex(hextoraw(t.c2)) from gbktable t where t.c0 =7;

###utl_raw_cast_to_raw
###utl_raw_cast_to_varchar2
###utl_raw_length
###utl_raw_bit_and
###utl_raw_bit_or
###utl_raw_bit_xor
###utl_raw_bit_complement
###utl_raw_reverse
###utl_raw_copies
###utl_raw_compare
###utl_raw_substr
###utl_raw_concat
##
##select UTL_RAW_CAST_TO_VARCHAR2( HEXTORAW(88)) from dual;
##select utl_raw_cast_to_raw( HEXTORAW(88)) from dual;

###select UTL_RAW_CAST_TO_VARCHAR2( HEXTORAW(t.c1)) from gbktable t where t.c0 =7;
###select UTL_RAW_CAST_TO_VARCHAR2( HEXTORAW(t.c2)) from gbktable t where t.c0 =7;
###select utl_raw_cast_to_raw( t.c1) from gbktable t where t.c0 =7;
###select utl_raw_cast_to_raw( HEXTORAW(t.c2)) from gbktable t where t.c0 =7;
###select utl_raw_cast_to_raw( HEXTORAW(t.c2)) from gbktable t where t.c0 =7;

###select utl_raw_length(t.c1) from gbktable t where t.c0 =7;
select utl_raw.length(t.c2) from gbktable t where t.c0 =7;

###select utl_raw_bit_and(t.c1,t.c2) from gbktable t where t.c0 =7;
###select utl_raw.bit_and(t.c1,t.c2) from gbktable t where t.c0 =7;

###select utl_raw_reverse(t.c1) from gbktable t where t.c0 =7;
###select utl_raw_concat(t.c1) from gbktable t where t.c0 =7;

###date_add
###date_sub
--disable_result_log

select sysdate, sysdate+3 from dual ;
select sysdate, add_months(sysdate,4) from dual ;
select sysdate, add_months(sysdate,12*2) from dual ;

--enable_result_log 

###pl_integer_checker --ORACLE无对比项
###pl_get_cursor_attr
###pl_get_sqlcode_sqlerrm
###pl_associative_index
###pl_collection_construct
###pl_object_construct
###sessiontimezone
###dbtimezone
###sys_extract_utc
###tz_offset
###from_tz --ORACLE无对比项

###--ORACLE无对比项
###dbms_lob_getlength
###dbms_lob_append
###dbms_lob_read
###dbms_lob_converttoblob
###dbms_lob_cast_clob_to_blob
###dbms_lob_convert_clob_charset
###--ORACLE无对比项

###select DBMS_LOB.GETLENGTH(c1), DBMS_LOB.GETLENGTH(c2), DBMS_LOB.GETLENGTH(c4) from gbktable;
###select DBMS_LOB.GETLENGTH(c1) from gbktable;
###select DBMS_LOB.GETLENGTH(c2) from gbktable;
###select DBMS_LOB.GETLENGTH(c4) from gbktable;
###select dbms_lob_getlength(c1) from gbktable;
###select dbms_lob_getlength(c2) from gbktable;
###select dbms_lob_cast_clob_to_blob(c1) from gbktable;


###ols_policy_create--ORACLE无对比项
###ols_policy_alter
###ols_policy_drop
###ols_policy_disable
###ols_policy_enable
###ols_level_create
###ols_level_alter
###ols_level_drop
###ols_label_create
###ols_label_alter
###ols_label_drop
###ols_table_policy_apply
###ols_table_policy_remove
###ols_table_policy_disable
###ols_table_policy_enable
###ols_user_set_levels
###ols_session_set_label
###ols_session_set_row_label
###ols_session_restore_default_label
###ols_session_label
###ols_session_row_label
###ols_label_value_cmp_le
###ols_label_value_check
###char_to_label
###label_to_char--ORACLE无对比项

##時間函數大部分與字符集無關
###add_months
###select t.c1, t.c2
###from gbktable t
###where t.c6 <= add_months(sysdate, -288);
###last_day
###next_day
###select NEXT_DAY(sysdate, 'MONDAY ') FROM DUAL;  
###select NEXT_DAY(sysdate, 'MONDAY ') FROM DUAL;  
###select last_day(sysdate) from dual;
######months_between
###to_yminterval
###to_dsinterval
###numtoyminterval
###numtodsinterval

###vsize
select vsize(t.c1) from gbktable t;
select vsize(t.c2) from gbktable t;

###ora_hash
###select ora_hash(t.c1,4294967295) from gbktable t ;
###select ora_hash(t.c1,42000) from gbktable t ;

###power
select power(2.5,2),power(1.5,0),power(20,-1) from dual;
select power(t.c1,2) from gbktable t where t.c0 = 7;
select power(t.c2,2) from gbktable t where t.c0 = 7;

###exp 暂不测试
###dump

###ln
###log

select exp(3),exp(-3),ln(20.0855369),ln(0.049787068) from dual;

select ln(t.c1) from gbktable t where t.c0 = 7;
select ln(t.c2) from gbktable t where t.c0 = 7;
select log(t.c1,2) from gbktable t where t.c0 = 7;
select log(t.c2,2) from gbktable t where t.c0 = 7;

###pl_seq_nextval暂不测试

###lengthc---当前227版本暂不支持
###substrc
###instrc

###select lengthc('新春快乐') from dual;
###select lengthc(t.c1) from gbktable  t;

###calc_partition_id--ORACLE无对比项
###pdml_partition_id
###sys_guid
###select sys_guid() from dual ;


###to_single_byte
###to_multi_byte

select to_single_byte(c0),to_single_byte(c1),to_single_byte(c2) from gbktable;
select to_single_byte(c0),to_single_byte(c1),to_single_byte(c2) from gbktable where to_single_byte(c1) like '中%';
select to_single_byte(c.c0),to_single_byte(c.c1)||' '||to_single_byte(c.c2),(select to_single_byte(t.c2) from gbktable t where to_single_byte(c.c0)= to_single_byte(t.c0) and c.c0= 1) as singleNEW from gbktable c ;
select to_multi_byte(c0),to_multi_byte(c1),to_multi_byte(c2) from gbktable;


###multiset  OB暂不支持

###select c1,c2 from gbktable  MULTISET UNION select c1,c2 from gbktable ; 
###select c1,c2 from gbktable  MULTISET INTERSECT select c1,c2 from gbktable ; 
 
###utl_i18n_string_to_raw
###utl_i18n_raw_to_char

select utl_i18n.string_to_raw(t.c1,'ZHS16GBK') from gbktable t;
select utl_i18n.string_to_raw(t.c2,'ZHS16GBK') from gbktable t;

select utl_i18n.raw_to_char(t.c8,'ZHS16GBK') from gbktable t;

###utl_inaddr_get_host_address    OB暂不支持
###utl_inaddr_get_host_name
###dbms_crypto_encrypt
###dbms_crypto_decrypt


###to_nchar   OB暂不支持


###pdb_debug_on--ORACLE无对比项
###pdb_debug_off
###pdb_attach_session
###pdb_detach_session
###pdb_initialize
###pdb_set_breakpoint
###pdb_print_backtrace
###pdb_continue
###pdb_show_breakpoints
###pdb_get_value
###pdb_delete_breakpoint
###pdb_enable_breakpoint
###pdb_disable_breakpoint
###pdb_set_timeout
###pdb_set_timeout_behaviour
###pdb_get_timeout_behaviour
###pdb_get_runtime_info
###calc_urowid--ORACLE无对比项
###set
###cardinality

create index new_idx on gbktable(c1);
select  /*+ cardinality(e 100)*/ * from gbktable e;

###coll_pred--ORACLE无对比项
###user_can_access_obj
###empty_clob
###empty_blob


insert into gbktable values(12,'中文测试012','aaabbb',empty_blob(),'中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd  hh24:mi:ss'),'a','300',999999);
insert into gbktable values(13,'中文测试013','aaabbb','300',empty_clob(),to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd  hh24:mi:ss'),'a','300',999999);
select t.c3,t.c4 from gbktable t;


--disable_warnings
--error 0,942
drop table gbktable CASCADE CONSTRAINTS purge;
--error 0,942
drop table emp CASCADE CONSTRAINTS purge;
--error 0,942
drop table t_partition_hash CASCADE CONSTRAINTS purge;
--enable_warnings
