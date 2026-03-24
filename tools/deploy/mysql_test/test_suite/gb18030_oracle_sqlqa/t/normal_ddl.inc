--echo  ##常规DDL测试DDL create table//gb18030-1-1-1

--disable_warnings
--error 0,942
drop table testtable1;
--enable_warnings

create table testtable1
(
c1 int,
srcuser1 varchar2(100) primary key,
srchost1 char(200),
dstuser1 nvarchar2(200),
dsthost1 nchar(200),
size2 int
);

insert into testtable1 values(1,'中文测试1','中文测试2','中文测试3','中文测试4',100);
insert into testtable1 values(3,01245,'中文测试9','中文测试10','中文测试11',300);
--error 1400
insert into testtable1 values(4,'','中文测试12','中文测试13','中文测试14',400);
insert into testtable1 values(5,'哈哈哈哈','中文测试12','中文测试13','中文测试14',500);
insert into testtable1 values(6,'hahaha','中文测试12','中文测试13','中文测试14',600);
--error 1400
insert into testtable1 values(7,'','','中文测试13','中文测试14',600);

select * from testtable1 order by c1;


--echo  ##常规DDL测试DDL create table//边界值(char/varchar2/nchar/nvarchar2（size）的size边界值)///gb18030-1-1-1-1

--disable_warnings
--error 0,942
drop table testtable1;
--enable_warnings

create table testtable1
(
c1 int,
srcuser1 varchar2(4000) primary key,
srchost1 char(2000),
dstuser1 nvarchar2(4000),
dsthost1 nchar(1000),
size2 int
);


###插入超限制数字
insert into testtable1 values(1,lpad('中文测试1a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',4000,'好'),lpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(2,lpad('中文测试2a',2000,'好'),lpad('中文测试2a',1000,'好'),lpad('中文测试2a',4000,'好'),lpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(3,rpad('中文测试3a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',4000,'好'),rpad('中文测试2a',1000,'好'),100);
insert into testtable1 values(4,rpad('中文测试4a',2000,'好'),rpad('中文测试2a',1000,'好'),rpad('中文测试2a',4000,'好'),rpad('中文测试2a',1000,'好'),100);

select * from testtable1 order by c1;


--echo  ##常规DDL测试,create table 虚拟列gb18030-1-1-1-2

--disable_warnings
--error 0,942
DROP TABLE stu CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE stu1 CASCADE CONSTRAINTS purge;
--enable_warnings

--echo  # 不支持alter添加约束, 不支持alter禁用和启用约束, 
--echo  # 不支持删除primary key , 不支持删除check时使用cascade 
--echo  # 验证虚拟列值和check的冲突, 虚拟列设置默认值
create table stu1(s1 varchar2(2), s2 varchar2(40) generated always as(s1+10), constraint s11_ch check(s2<=11)) compress;
--echo  ##常规DDL测试,create table comment gb18030-1-1-1-3
##bug：31553770- 【GB18030】comment中带中文时，OB 有报错，ORACLE正常
##comment on table stu1 is ' 中 ';
##comment on table stu1 is '中';
--error 12899
insert into stu1(s1) values('好的');
###GB18030租户时，报错1722
--error 12899,1722
insert into stu1(s1) values('好');
insert into stu1(s1) values(1);
--echo  # 验证虚拟列值和check约束
--error 02290
insert into stu1(s1) values(2);

--echo  ##常规DDL测试,create table 默认值gb18030-1-1-1-5

##bug：
###create table stu(s1 varchar2(2) primary key, s2 varchar2(40) generated always as (s1+10), s3 varchar2(20) default '好的', constraint s1_ch check(s1>5));
create table stu(s1 varchar2(2) primary key, s2 varchar2(40) generated always as (s1+10), s3 varchar2(20) default '10', constraint s1_ch check(s1>5));

##comment暂不支持
##comment on table stu is '关联表';
##comment on column stu.s1 is '分区字段';

# 删除check
--echo  # 验证异常情况,给虚列写值
--error 600,54013
insert into stu(s1,s2,s3) values(5, 10, 1);
--echo  # 验证异常情况,check约束
--error 02290
insert into stu(s1,s3) values(5, 1);
--echo  # 验证缺省虚拟列名的写入
--error 600
insert into stu values(6, 1);
insert into stu(s1) values(7);
--echo  # 删除约束,写入值测试
alter table stu drop constraint s1_ch;
insert into stu(s1,s3) values(5, 1);
insert into stu(s1) values(10);
###--error 00001
###insert into stu(s1) values(10);

--disable_warnings
--error 0,942
DROP TABLE stu CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE stu1 CASCADE CONSTRAINTS purge;
--enable_warnings

--echo  # 生成列拼接中文
create table stu1(s1 varchar2(2), s2 varchar2(40) generated always as(s1+'好的'), constraint s11_ch check(s2<=11)) compress;
--error 01722
insert into stu1(s1) values(1);

create table stu(s1 varchar2(20), s2 varchar2(40) generated always as(concat(s1,'好的'))) compress;
insert into stu(s1) values('中文啊1+b');
select * from stu order by s1;


--disable_warnings
--error 0,942
DROP TABLE stu CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE stu1 CASCADE CONSTRAINTS purge;
--enable_warnings



--echo  ##################使用虚拟列作为外键和分区列, 创建索引##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--enable_warnings

--echo  # 
create table stu1(s1 varchar2(2) primary key, s2 varchar2(20)) compress;
insert into stu1(s1) values(2);
insert into stu1(s1) values(4);
insert into stu1(s1) values(6);

create table stu(s1 varchar2(20), s2 varchar2(40) generated always as(concat(s1,'好的')), constraint stu_pk primary key(s1)) partition by hash(s2) partitions 10;
insert into stu(s1) values(1);
insert into stu(s1) values(2);
--echo  # foreign key之外的值
insert into stu(s1) values(4);
create index stu_s1ind on stu(s2);
--error 01408
create index stu_sind on stu(s1);

--echo  # select中的应用
--error 01722
select s1/8, s2/6 from stu where s2>0;

--echo  # dml中应用
--error 600,54013
insert into stu values(3,8);
--error 600,54017
update stu set s2=4 where s1=2;
--error 01722
delete from stu where s2=2;
--error 600,54013
insert into stu values(1,2);

--echo  # 新增个虚拟列
###虚拟列alter 新增时，有中文则报错
###bug：31553791- 【GB18030】虚拟列ALTER新增列带中文时，OB 有报错，ORACLE正常
###alter table stu add s3 generated always as(concat(s1,'好的啊')) virtual;
###select * from stu;


--echo  ##常规DDL测试, drop gb18030-1-3//gb18030-1-3-1
--echo  ##################虚拟列的视图##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu1;
--error 0,942
drop view view_stu;
--error 0,942
drop view view_stu1;
--enable_warnings

--echo  # 创建基表//空值, null及非法字符
###31553791- 【GB18030】虚拟列ALTER新增列带中文时，OB 有报错，ORACLE正常
create table stu1(s1 varchar2(2), s2 varchar2(20) generated always as(concat(s1,'好的啊')));
insert into stu1(s1) values(1);
insert into stu1(s1) values('');
insert into stu1(s1) values(null);
insert into stu1(s1) values('0');
select s2 from stu1 order by s2 desc;
select s2 from stu1 order by s2 asc;


--echo  ##常规DDL测试,create view gb18030-1-1-3，gb18030-1-1-3-1
--echo  ##常规DDL测试,create view gb18030-1-1-3，gb18030-1-1-3-2
--echo  ##常规DDL测试,create view gb18030-1-1-3，gb18030-1-1-3-3
--echo  # 创建两个视图(一个使用原列名, 一个重命名)
create view view_stu as select * from stu1;
create view view_stu1(s1,s2) as select * from stu1;

desc view_stu;
desc view_stu1;

--echo  # 视图中对nchar及其虚拟列的dml操作
insert into stu1(s1) values(3);
insert into view_stu(s1) values(3);
insert into view_stu1(s1) values(3);
insert into view_stu(s1) values(20);
insert into view_stu1(s1) values(20);
--error 00600,54017
update view_stu set s2=5 where s1=3;
--error 00600,54017
update view_stu1 set s2=5 where s1=3;

select * from stu1;
select * from view_stu;
select * from view_stu1;

--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu1;
--error 0,942
drop view view_stu;
--error 0,942
drop view view_stu1;
--enable_warnings



--echo  ##################基础功能测试##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--enable_warnings

--echo  # 指定位数的容量测试, 0及异常数字及非法值
create table stu(s1 varchar2(6));
insert into stu(s1) values('测试');
--error 0,12899
insert into stu(s1) values('测试,');
insert into stu values('   ');
--error 0,12899
insert into stu values('测测试');
insert into stu values('  ');
insert into stu values(null);

###错误提示中有中文显示乱码
--disable_result_log
--error 00904
insert into stu values(好的);
--enable_result_log

--error 00900
insert into stu values(,,,);

insert into stu values(',,,');
insert into stu values('\\\\');
insert into stu values('\\');
insert into stu values('**');
insert into stu values('$$');
insert into stu values('%%');
insert into stu values('''');
insert into stu values('''''');
insert into stu values(123);
insert into stu values('abc');
insert into stu values(''' ');
insert into stu values(' '' ');
insert into stu values('');
--error 01723
create table stu1(s1 varchar2(0));
--error 00900
create table stu1(s1 varchar2(-10));
--error 00900
create table stu1(s1 varchar2('5'));

--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--enable_warnings

--disable_warnings
--error 0,600,942
DROP TABLE t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

--echo #建表时用constraint//create table
--echo  ##常规DDL测试,create table 约束 gb18030-1-1-1-4
--echo  ##常规DDL测试,create table 关键字gb18030-1-1-1-6
--echo  ##常规DDL测试,create table 分区gb18030-1-1-1-7

create table t_utf16(c1 varchar2(200) not null unique, c2 varchar2(200) not null, c3 varchar2(200) default '0FF' not null, c4 generated always as(c2 || c3), c5 varchar2(200) unique,  c6 varchar2(200) unique, c7 varchar2(200) not null, c8 varchar2(200) default '0FF' not null, c9 varchar2(500) generated always as(c7 || c8),c10 varchar2(200) unique, c11 varchar2(200) not null, c12 varchar2(200) default '0FF' not null, c13 varchar2(500) generated always as(c11 || c12),constraint pks1_c6 primary key(c1,c6,c10), constraint c1_check check((c2 || c7 || c11) not in ('中国','中文测试','好的','可以abc','0E0E0E'))) pctfree 3  pctused 3 initrans 3  enable row movement PARTITION BY hash(c1) SUBPARTITION BY range(c1) SUBPARTITION TEMPLATE (SUBPARTITION  t_utf16_p0 VALUES LESS THAN ('中1aaaaaaaaaaaaa'),SUBPARTITION  t_utf16_p1 VALUES LESS THAN ('中2aaaaaaaaaaaaa11'),SUBPARTITION  t_utf16_p2 VALUES LESS THAN ('中3aaaaaaaaaaaaa1122'),SUBPARTITION  t_utf16_p3 VALUES LESS THAN ('中4aaaaaaaaaaaaa112244'),SUBPARTITION  t_utf16_p4 VALUES LESS THAN ('中5aaaaaaaaaaaaa11224455'),SUBPARTITION  t_utf16_pp VALUES LESS THAN (MAXVALUE)) PARTITIONS 10;


--echo  ##常规DDL测试,查看建表结果 gb18030-1-1-5 gb18030-1-1-5-1
--disable_query_log
--disable_result_log

select * 
from user_tables 
where table_Name='T_UTF16';

select * from DBA_PART_TABLES a where a.table_name=upper('T_UTF16');

--enable_result_log
--enable_query_log


--echo  ##常规DDL测试,查看建表结果 gb18030-1-1-5 gb18030-1-1-5-2
desc t_utf16;

insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('1234','1','1234','1234','2234','2','2234','3234','3','3234');


--disable_warnings
--error 0,600,942
DROP TABLE t_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

create table t_utf16(c1 varchar2(200) not null unique, c2 varchar2(200) not null, c3 varchar2(200) default '0FF' not null, c4 generated always as(c2 || c3), c5 varchar2(200) unique,  c6 varchar2(200) unique, c7 varchar2(200) not null, c8 varchar2(200) default '0FF' not null, c9 varchar2(500) generated always as(c7 || c8),c10 varchar2(200) unique, c11 varchar2(200) not null, c12 varchar2(200) default '0FF' not null, c13 varchar2(500) generated always as(c11 || c12),constraint pks1_c6 primary key(c1,c6,c10), constraint c1_check check((c2 || c7 || c11) not in ('中国','中文测试','好的','可以abc','0E0E0E'))) pctfree 3  pctused 3 initrans 3  enable row movement PARTITION BY hash(c1) SUBPARTITION BY range(c1) SUBPARTITION TEMPLATE (SUBPARTITION  t_utf16_p0 VALUES LESS THAN ('中1aaaaaaaaaaaaa'),SUBPARTITION  t_utf16_p1 VALUES LESS THAN ('中2aaaaaaaaaaaaa11'),SUBPARTITION  t_utf16_p2 VALUES LESS THAN ('中3aaaaaaaaaaaaa1122'),SUBPARTITION  t_utf16_p3 VALUES LESS THAN ('中4aaaaaaaaaaaaa112244'),SUBPARTITION  t_utf16_p4 VALUES LESS THAN ('中5aaaaaaaaaaaaa11224455'),SUBPARTITION  t_utf16_pp VALUES LESS THAN (MAXVALUE)) PARTITIONS 10;


--disable_warnings
--error 0,600,942
DROP TABLE t_utf16_tmp CASCADE CONSTRAINTS purge;
--enable_warnings

create table t_utf16_tmp(b1 varchar2(2000));

set session ob_query_timeout = 400000000;


--echo  ##常规DDL测试,create index gb18030-1-1-2//gb18030-1-1-2-1
--echo  ##常规DDL测试,create index gb18030-1-1-2//gb18030-1-1-2-2
--echo  ##常规DDL测试,create index gb18030-1-1-2//gb18030-1-1-2-3
--echo  ##常规DDL测试,create index gb18030-1-1-2//gb18030-1-1-2-4
--echo #create index
--error 600
create unique index t_utf16_func_idx on t_utf16(UPPER(C5)) local visible;
###OB有内部报错
###create unique index t_utf16_idx_local on t_utf16(c1,c6 asc) local visible;
create index t_utf16_idx_global on t_utf16(c8 asc,c12) global PARTITION by hash(c8,c12) PARTITIONS 20;
set session ob_query_timeout = 10000000;  


--echo #create view gb18030-1-1-3-1
create or replace  view t_utf16_view(vc1,vc2,vc3,vc5,vc6,vc7,vc8,vc10,vc11,vc12) as select c1,c2,c3, c5, c6,c7,c8,c10,c11,c12 from t_utf16;


--echo #create synonym

create or replace  synonym t_utf16_synonym for t_utf16;
--echo  ##常规DDL测试, drop gb18030-1-3//gb18030-1-3-3
--error 0,600
drop synonym t_utf16_synonym;
create or replace  synonym t_utf16_synonym for t_utf16;

create or replace public synonym t_utf16_synonym1 for t_utf16; 

create or replace  synonym t_utf16_synonym_v for t_utf16_view; 

create or replace public synonym t_utf16_synonym_v1 for t_utf16_view;
  
create or replace  synonym t_utf16_synonym_s for t_utf16_synonym1;
  
create or replace public synonym t_utf16_synonym_s1 for t_utf16_synonym1;

insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文1','1','1234','1234','2234','2','2234','3234','3','3234');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文2','2','1234','2234','22234','2','2234','23234','3','3234');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文3','1','1','1','2','2','2','3','3','3');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文4','1','11','11','22','2','22','33','3','33');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文5','ff','11','111','222','2','22','333','3','33');

insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文6','ff','11','1111','2222','fff','22','3333','3','33');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文7','ff','11','1112','2223','ffff','22','3334','3','33');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文8','ff','11','1113','2224','ffff','22','3335','ffffff','33');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('0123456789abcdefABCDEF', 'ff','11','1125','2235','ffff','22','1023456789abcdefABCDEF','ffffff','33');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values(CONCAT('中文','中国的文字'), 'ff','11','1126','2236','ffff','22','21023456789abcdefABCDEF','ffffff','33');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values(100 || 'ff', 'ff','11','1127','2237','ffff','22','3237','ffffff','33');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values( (1|| 'f') || (1+2) || 1 ||1 || 1 ||1||1||1||1||1||1||'1'||1||1||1||'11'||(3*2)||(5-2)||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||1||1||1||(2||4)||1||1||1||1||1||1||1||1||(2||4), 'ff','11','1150', '2250','ffff','22','3250','ffffff','33');
--error 600
insert into t_utf16 values('1128','ff','11','1128','2238','ffff','22','3238','ffffff','33');

select c1,c2,c3,c5,c6,c7,c8,c10,c11,c12 from t_utf16 order by c1;
###select c1 || c6 || c10 from t_utf16;
select c2 || c5 from t_utf16 order by 1;


###--echo ### with as测试;OB报错00942
###with ttest_utf16 as (select c1 from t_utf16) ,ttest_utf162 as (select c6 from t_utf16) select r1.c1, r2.c6 from ttest_utf16 r1, ttest_utf162 r2 where r1.c1 = r2.c6;
###--error 0,904
###with ttest_utf16(c1,c2,c3,c5,c4) as ((select c1,c2,c3,c5,c4 from t_utf16 where c2 = '02') union all(select r1.c1, r1.c2, r1.c3, r1.c5,r1.c4 from ttest_utf16 r1, t_utf16 r2 where r2.c1 = r1.c2)) select * from ttest_utf16;
###
###--error 0,904
###with ttest_utf16(c1,c2,c3,c5,c4) as ((select c1,c2,c3,c5,c4 from t_utf16 where c2 = '01') union all(select r1.c1, r1.c2, r1.c3, r1.c5,r1.c4 from ttest_utf16 r1, t_utf16 r2 where r2.c1 = r1.c2)) select * from ttest_utf16;
###--error 0,904
###with ttest_utf16(c1,c2,c3,c5,c4) as ((select c1,c2,c3,c5,c4 from t_utf16 where c2 = '01') union all(select r1.c1, r1.c2, r1.c3, r1.c5,r1.c4 from ttest_utf16 r1, t_utf16 r2 where r2.c1 = r1.c2)) SEARCH DEPTH FIRST BY c1 SET order1 CYCLE c1 SET is_cycle TO 'Y' DEFAULT 'N' select * from ttest_utf16;


--echo ### 视图
select vc1,vc2,vc3  from t_utf16_view where vc2 like ('%F');

--echo  ##常规DDL测试,create synonym gb18030-1-1-4-1
--echo ### 同义词
select c1,c2,c3  from t_utf16_synonym_s1 where c2 like ('%F');


--echo  ##常规DDL测试, alter table gb18030-1-2//gb18030-1-2-1
--echo  ##常规DDL测试, alter table gb18030-1-2//gb18030-1-2-2
--echo  ##常规DDL测试, alter table gb18030-1-2//gb18030-1-2-3
--echo  ##常规DDL测试, alter table gb18030-1-2//gb18030-1-2-4
--echo ###aler table
--echo ###add varchar2 attribut###
alter table t_utf16 add (c1add varchar2(200) default 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' not null, c2add generated always as(c3 || c1));
###alter table t_utf16 add (c3add varchar2(200) default 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' not null, c4add generated always as(c3 || c1));

--echo ###modify varchar2 attribut###
alter table t_utf16 modify (c3 varchar2(200));
alter table t_utf16 modify (c3 varchar2(200));

##alter table t_utf16 modify (c5 varchar2(200 char) default '00AAAAAA' NULL);
##alter table t_utf16 modify (c5 varchar2(200 byte) default '00AAAAAA' NULL);


--echo  ##常规DDL测试, drop gb18030-1-3//gb18030-1-3-4
--echo ###drop varchar2 attribut
--echo #删除虚拟列相关的列
alter table t_utf16 drop column c1add;

--echo #多次删除
--error 0,1418
alter table t_utf16 drop column c1add;

--echo #删除虚拟列
alter table t_utf16 drop column c2add;

--echo #分区列
--error 600,12984
alter table t_utf16 drop column c1;

--echo #删除索引列
--error 0,1418
alter table t_utf16 drop column c4;

--echo #删除外键列
--error 0,600,54031
alter table t_utf16 drop column c3;

--echo #删除check相关的列
###OB不报错，ORACLE报错。已知问题，OB高版本3.1以上，修复
--error 0,12991
alter table t_utf16 drop column c2;

--disable_warnings
--error 0,600,942
DROP TABLE tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

create table tt_utf16 (c1 varchar2(200));

--echo #删除唯一的列
--error 54039
alter table tt_utf16 drop column c1;


--echo #删除主键列
--disable_warnings
--error 0,600,942
DROP TABLE tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings


create table tt_utf16 (c1 varchar2(200) primary key, c2 varchar2(200));
--error 0,600,942
alter table tt_utf16 drop column c1;



--echo #删除带外键列
--disable_warnings
--error 0,600,942
drop table ttt_utf16 CASCADE CONSTRAINTS purge;
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

create table tt_utf16 (c1 varchar2(200) primary key, c2 varchar2(200));
create table ttt_utf16 (c1 varchar2(200) primary key, c2 varchar2(200), constraint fk_tt foreign key(c1) REFERENCES tt_utf16(c1));
--error 12992
alter table tt_utf16 drop column c1;
alter table ttt_utf16 drop column c2;

--disable_warnings
--error 0,600,942
drop table ttt_utf16 CASCADE CONSTRAINTS purge;
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings


--echo ###rename column name###
--echo #唯一的列

create table tt_utf16 (c1 varchar2(200));
alter table tt_utf16 rename column c1 to cc;
--error 904
alter table tt_utf16 rename column c1 to cc;

alter table tt_utf16 add (c1 varchar2(200));
--error 600
alter table tt_utf16 rename column c1 to cc;
alter table tt_utf16 rename column c1 to cc1;

--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

create table tt_utf16 (c1 varchar2(200));
alter table tt_utf16 rename column c1 to cc;
--error 904
alter table tt_utf16 rename column c1 to cc;
alter table tt_utf16 add (c1 varchar2(200));
--error 600
alter table tt_utf16 rename column c1 to cc;
alter table tt_utf16 rename column c1 to cc1;



--echo #rename unique列
--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table tt_utf16 (c1 varchar2(200) unique, c2 varchar2(200));
alter table tt_utf16 rename column c1 to cc;

--echo #rename primary key列
--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table tt_utf16 (c1 varchar2(200) primary key, c2 varchar2(200));
alter table tt_utf16 rename column c1 to cc;


--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table tt_utf16 (c1 varchar2(200), c2 varchar2(200), c3 varchar2(200), constraint pk_tt_check primary key(c1,c2));
alter table tt_utf16 rename column c1 to cc;


--echo #rename foreign key列
--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table tt_utf16 (c1 varchar2(200) default 'aaa' not null unique, c2 varchar2(200));

--disable_warnings
--error 0,600,942
drop table ttt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table ttt_utf16 (c1 varchar2(200), c2 varchar2(200), constraint fk_tt foreign key(c1) REFERENCES tt_utf16(c1));
alter table tt_utf16 rename column c1 to cc;

--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

create table tt_utf16 (c1 varchar2(200), c2 varchar2(200), constraint unique_test unique(c1,c2));
--disable_warnings
--error 0,600,942
drop table ttt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table ttt_utf16 (c1 varchar2(200), c2 varchar2(200), constraint fk_tt foreign key(c1,c2) REFERENCES tt_utf16(c1,c2));
alter table tt_utf16 rename column c1 to cc;


--echo #rename 分区列
--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table tt_utf16 (c1 varchar2(200),c2 varchar2(200)) partition by hash(c1) subpartition by list(c2) SUBPARTITION TEMPLATE (SUBPARTITION  t_utf16_p0 VALUES ('aaaaa','a','aa','aaa','aaaa')) partitions 3;
alter table tt_utf16 rename column c1 to cc;
alter table tt_utf16 rename column c2 to cc2;


--echo #rename check列
--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table tt_utf16 (c1 varchar2(200),c2 varchar2(200), constraint tt_check check(c1 < 'aaaaa'));
alter table tt_utf16 rename column c1 to cc;


##truncate table
truncate table tt_utf16;
--disable_warnings
--error 0,600,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings
create table tt_utf16 (c1 varchar2(200),c2 varchar2(200),c3 varchar2(200), constraint tt_check check(c1 || c2 < 'aaaaa'));
alter table tt_utf16 rename column c1 to cc;

--echo  ##常规DDL测试, drop gb18030-1-3//gb18030-1-3-2
##drop table /drop index
--echo #rename 索引列
--disable_warnings
--error 0,1418
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--error 0,1418
drop index tt_idx_local;
--error 0,1418
drop index tt_idx_g;
--enable_warnings
create table tt_utf16 (c1 varchar2(200 ) primary key,c2 varchar2(200),c3 varchar2(200)) partition by hash(c1)  partitions 3;
create index tt_idx_local on tt_utf16(c2) local;
create unique index tt_idx_g on tt_utf16(c3) global PARTITION by hash(c3) PARTITIONS 10;
alter table tt_utf16 rename column c1 to cc;
--error 600
alter table tt_utf16 rename column c2 to cc;
alter table tt_utf16 rename column c2 to cc1;
alter table tt_utf16 rename column c3 to cc3;
--disable_warnings
--error 0,1418
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--error 0,1418
drop index tt_idx_local;
--error 0,1418
drop index tt_idx_g;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_datatype_join4;
--error 0,00942
drop table gbk_datatype_join5;
--error 0,00942
drop table gbk_datatype_join6;
--error 0,00942
drop table gbk_datatype_join7;
--enable_warnings
create table gbk_datatype_join4(c1 varchar2(4000));
insert into gbk_datatype_join4 values('曝料：明年iPhone要剪刘海儿；俞敏洪：当你工资比同学少一半，证明生命已经浪费一半……晚间回顾一下有哪些不容错过的科技新闻和网友的神回复。
卢伟冰回怼余承东：外国车牌贴手机上多卖一万是啥实用价值
9月26日，华为在国内发布了Mate 30系列产品，提到刚发布的小米5G环绕屏MIX Aphla概念机时，---@余承东 评价称只是概念手机，没有实用价值。对此，小米---@卢伟冰 发微博表示，如果说环绕屏没有“实用价值”，
那么把一个外国车牌贴在手机上就多卖1万块的“实用价值”是什么？---@下午茶食乜好：有一说一，确实。---@i猪喊你：嘴上说着没有实用价值，转过头抄华为施华洛世奇手机壳，499一个。---@歪闷4：看成卢本伟了。
俞敏洪：当你工资比同学少一半，证明生命已经浪费一半
近日，俞敏洪在“中国大学生自强之星”分享会上表示，钱是一个人的能力证明，当你工资比同学少一半时，你的生命已经浪费了一半。每个人都只有24个小时，但有的人能把时间不断累加让自己变成一种更加自由的状态。---@SmilEdo：为他们公关掬一把同情的泪。---@围观群众张先生：你的资产不到马云的十分之一，你已经浪费了绝大部分的生命。---@汤米饭215：科学家含泪跪拜企业家。
曝料：明年iPhone要剪刘海儿？还能保留面部识别技术？---@BenGeskin 推特曝料称，苹果已设计了一款没有“刘海儿”的6.7英寸屏幕iPhone，用于人脸识别的原深感镜头等部件将被缩小，安置在显示屏上方的边
中。Geskin做了一张示意图来说明所谓未来款iPhone，他说苹果计划在2020年发布这产品。');

create table gbk_datatype_join5(c1 int,c2 varchar(20)) partition by hash(c2);
create table gbk_datatype_join6(c1 int,c2 char(20)) partition by hash(c2);
create table gbk_datatype_join7(c1 int,c2 varchar2(20)) partition by hash(c2);
delimiter //;
begin for i in 1..1400 loop insert into gbk_datatype_join5(c1,c2) select i,substr(c1,i,2) from gbk_datatype_join4; end loop;end;
//
begin for i in 1..1400 loop insert into gbk_datatype_join6(c1,c2) select i,substr(c1,mod(i,100),2) from gbk_datatype_join4; end loop;end;
//
begin for i in 1..1400 loop insert into gbk_datatype_join7(c1,c2) select i,substr(c1,mod(i,100)+50,2) from gbk_datatype_join4; end loop;end;
//
delimiter ;//
select count(*) from gbk_datatype_join5 表1,gbk_datatype_join6 表2,gbk_datatype_join7 表3 where 表1.c2=表2.c2 and 表1.c2=表3.c2;
select cl,clb,count(*) from (select length(c2) cl,lengthb(c2) clb from gbk_datatype_join5) group by cl,clb order by clb desc;

select distinct a.c1 as c1,a.c2 as c2 from gbk_datatype_join5 a join (select distinct a.c1,a.c2 from gbk_datatype_join6 a,gbk_datatype_join7 b where a.c2=b.c2) b on a.c2=b.c2 order by c1 desc;

--disable_warnings
--error 0,00942
drop table gbk_datatype_join4;
--error 0,00942
drop table gbk_datatype_join5;
--error 0,00942
drop table gbk_datatype_join6;
--error 0,00942
drop table gbk_datatype_join7;
--enable_warnings
