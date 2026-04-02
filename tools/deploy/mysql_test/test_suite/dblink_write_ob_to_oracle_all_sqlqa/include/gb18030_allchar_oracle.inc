SET NAMES UTF8;
--disable_warnings
--error 0,00942
drop table gbk_dml_001;
--error 0,02289
drop sequence gbk_insert_seq;
--enable_warnings

###GB18030编码：2000年3月17日发布的汉字编码国家标准，是对GBK编码的扩充，覆盖中文、日文、朝鲜语和中国少数民族文字，
###其中收录27484个汉字。GB18030字符集采用单字节、双字节和四字节三种方式对字符编码。兼容GBK和GB2312字符集。
create sequence gbk_insert_seq increment by 1 start with 1 nomaxvalue minvalue 0 nocycle nocache;
create table gbk_dml_001(c0 varchar(20),c1 varchar2(20),c2 char(20),c4 nchar(10),c5 nvarchar2(10),c3 int);

--disable_warnings
--error 0,942
drop table testtable1;
--enable_warnings

create table testtable1
(
c1 int,
srcuser1 varchar2(4000),
srchost1 char(2000),
dstuser1 nvarchar2(2000),
dsthost1 nchar(1000),
size2 int
);

--disable_warnings
--error 0,942
DROP TABLE stu1215 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE stu1216 CASCADE CONSTRAINTS purge;
--enable_warnings

--echo  # 生成列拼接中文
create table stu1216(s1 varchar2(2), s2 varchar2(40) generated always as(s1+'好的'), constraint s11_ch check(s2<=11)) compress;
create table stu1215(s1 varchar2(20), s2 varchar2(40) generated always as(concat(s1,'好的'))) compress;

--disable_warnings
--error 0,942
DROP TABLE stu1217;
--enable_warnings

--echo  # 创建基表//空值, null及非法字符
create table stu1217(s1 varchar2(2), s2 varchar2(20) generated always as(concat(s1,'好的啊')));
insert into stu1217(s1) values(1);
insert into stu1217(s1) values('');
insert into stu1217(s1) values(null);
insert into stu1217(s1) values('0');
select s2 from stu1217 order by s2 desc;
select s2 from stu1217 order by s2 asc;

--disable_warnings
--error 0,942
DROP TABLE stu1218;
--error 0,942
--enable_warnings

--echo  # 指定位数的容量测试, 0及异常数字及非法值
create table stu1218(s1 varchar2(6));


--disable_warnings
--error 0,942
drop table stu1219;
--error 0,942
--enable_warnings

create table stu1219(s1 varchar2(9) , s2 varchar2(9) , s3 varchar2(9));
insert into stu1219 values('韩国', '韓国', '??');
insert into stu1219 values(260, 'abc', '1a?');
insert into stu1219 values(028, '0ab', '01a');
insert into stu1219 values('讋讋讋', '詟詟詟', '詟詟詟');
insert into stu1219 values('???', '???', '???');
