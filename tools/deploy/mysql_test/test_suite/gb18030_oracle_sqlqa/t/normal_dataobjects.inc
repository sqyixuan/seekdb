--echo  ##数据对象gb18030-3//gb18030-3-4
--echo  ##################同义词中的应用##################
--disable_warnings
--error 0,942
drop table stu;
--error 0,600
drop synonym syn;
--enable_warnings


--echo  # 创建表
--echo  # 生成列不支持作为primary key, 已知bug 
--error 00600
create table stu(s1 varchar2(2), s2 varchar2(4) unique not null, s3 varchar2(20) generated always as (s2*1.5), check(mod(s2,2)!=0), constraint stu_pk primary key(s1,s2,s3))partition by hash(s2) partitions 5;
create table stu(s1 varchar2(2), s2 varchar2(4) unique not null, s3 varchar2(20) generated always as (s2*1.5), check(mod(s2,2)!=0))partition by hash(s2) partitions 5;

--disable_warnings
--error 0,942
drop table stu;
--enable_warnings

create table stu(s1 varchar2(2), s2 varchar2(4) unique not null, s3 varchar2(20) generated always as (s2*1.5), check(mod(s2,2)!=0), constraint stu_pk primary key(s1,s2))partition by hash(s2) partitions 5;

--error 02290
insert into stu(s1,s2) values(1,2);
insert into stu(s1,s2) values(1,3);



--disable_warnings
--error 0,942
drop table stu;
--enable_warnings

--echo  # 建表, 用于同义词的基表
create table stu(s1 varchar2(2), s2 varchar2(4) unique not null, s3 varchar2(20) generated always as (s2*1.5), check(s2>-1), constraint stu_pk primary key(s1,s2))partition by hash(s2) partitions 5;

--error 02290
insert into stu(s1,s2) values(1,-2);
delete from stu;
insert into stu(s1,s2) values(1,3);
insert into stu(s1,s2) values(1,5);

--echo  # 创建同义词,进行增删改查
create or replace public synonym syn for stu;
select * from syn order by s1,s2;
--echo  # insert正常值
insert into syn(s1,s2) values(1,6);
insert into syn(s1,s2) values(001,7);
insert into syn(s1,s2) values('ab',8);
--echo  # insert异常值, 受限s2的unique
--disable_result_log
--error 0001
insert into syn(s1,s2) values('ab',7);
--enable_result_log
--echo  # insert异常值, 受限s1的字符长度
--error 12899
insert into syn(s1,s2) values('abc',9);
--error 01400
insert into syn(s1,s2) values('66','');
--error 12899
insert into syn(s1,s2) values('66','666666666');
--error 600,54013
insert into syn(s1,s2,s3) values('66','66',1.5);


--echo  # 同义词中的update
alter table stu enable row movement;

--echo  # update生成列的值
--error 600,54017
update syn set s3=4.5 where s1=1;
--error 600,54017
update stu set s3=4.5 where s1=1;

--echo  # update s1超限2
--error 12899
update syn set s1='aaaa' where s3=12;
--error 12899
update stu set s1='aaaa' where s3=12;

--echo  # update s2列的unique
--disable_result_log
--error 00001
update syn set s2=7 where s3=12;
--error 00001
update stu set s2=7 where s3=12;
--enable_result_log

--echo  # update s2列的not null
--error 01400
update syn set s2=null where s3=12;
--error 01400
update stu set s2='' where s3=12;


--echo  ##################同义词中的foreign key##################
--disable_warnings
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop synonym syn;
--enable_warnings

create table stu1(s1 varchar2(2) primary key, check(s1>any(1, 2, 3, 4)));
insert into stu1 values(5);
insert into stu1 values(4);
insert into stu1 values(3);


create table stu2(s1 varchar2(2), s2 varchar2(6), check(s2!=4), s3 varchar2(20) generated always as(s1+1), constraint stu2_pk primary key(s1,s2))partition by hash(s2,s1) partitions 5;

insert into stu2(s1,s2) values(1,5);
alter table stu2 enable row movement;
update stu2 set s2=3 where s1=1;
--error 02290
insert into stu2(s1,s2) values(1,4);


--echo  # 创建同义词, 验证foreign key
create or replace public synonym syn for stu2;
--error 600,54013
insert into syn(s1,s2,s3) values(2,5,5);
--error 600,54013
insert into syn values(2,5,5);
--echo  # 验证外键是否受约束
insert into syn(s1,s2) values(3,1);
###--error 00001
###insert into stu2(s1,s2) values(3,1);
update stu2 set s1=6;
--echo  # 验证check是否受约束
insert into syn(s1,s2) values(2,5);
insert into syn(s1,s2) values(3,-1);
--error 02290
insert into syn(s1,s2) values(3,4);
select to_char(s1),to_char(s2),to_char(s3) from syn order by to_char(s1);

--echo  ##数据对象gb18030-3//gb18030-3-3
--echo  ##################package包中的应用##################
--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop package my_pkg;
--enable_warnings

--echo  # 创建表stu1, stu2 foreign key stu1
create table stu1(s1 varchar2(2) primary key, s2 varchar2(4) unique, s3 varchar2(20) generated always as(s2*0.001), check(s2>0 and s1>0));

create index stu1_s3 on stu1(s3);
--error 01408
create index stu1_s2 on stu1(s2);

insert into stu1(s1,s2) values(2,2);
insert into stu1(s1,s2) values(3,3);
insert into stu1(s1,s2) values(4,4);
insert into stu1(s1,s2) values(20,20);

--echo  # 创建表stu2, s2外键关联stu1的s1字段
create table stu2(s1 varchar2(2), s2 varchar2(5), s3 varchar2(20) generated always as(s2/5), check(s1>2 and s2<20), constraint stu2_pk primary key(s1,s2))partition by hash(s2) partitions 4;
create index stu2_s1 on stu2(s1);
create index stu2_s2 on stu2(s2);
create index stu2_s3 on stu2(s3);

--echo  # 验证预期失败, s1只能写入数字
--error 01722
insert into stu2(s1,s2) values('a',0);

--echo  # 验证预期失败, 外键不存在数字
--error 02290
insert into stu2(s1,s2) values(1,0);

--echo  # 验证预期失败, s1 check受限
--error 02290
insert into stu2(s1,s2) values(1,2);

--echo  # 写入正常值
insert into stu2(s1,s2) values(3,2);
insert into stu2(s1,s2) values(6,3);
insert into stu2(s1,s2) values(9,4);
insert into stu2(s1,s2) values(99,4);


delimiter /;
--echo  # 创建包头
create or replace package my_pkg is
-- stu2表的insert操作
procedure pro(x varchar2, y varchar2);
-- stu1表的insert操作
procedure pro2(x varchar2, y varchar2);
-- update stu2表s1字段
procedure pro4(v1 varchar2, v2 varchar2);
-- 删除stu1内容
procedure pro5(x varchar2);
-- 删除stu2内容
procedure pro6(x varchar2);
-- update stu2表s2字段
procedure pro7(v1 varchar2, v2 varchar2);
end;
/

--echo  # 创建包体
create or replace package body my_pkg is
procedure pro(x varchar2, y varchar2) is
begin
insert into stu2(s1,s2) values(x,y);
end;
procedure pro2(x varchar2, y varchar2) is
begin
insert into stu1(s1,s2) values(x,y);
end;
procedure pro4(v1 varchar2, v2 varchar2) is
begin
update stu2 set s1=v1 where s2=v2;
end;
procedure pro5(x varchar2) is
begin
delete from stu1;
end;
procedure pro6(x varchar2) is
begin
delete from stu2 where s1=x;
end;
procedure pro7(v1 varchar2, v2 varchar2) is
begin
update stu2 set s2=v1 where s2=v2;
end;
end;
/
--disable_query_log
--disable_result_log
--echo  # 调用包处理数据
--echo  # 插入正常值
begin
my_pkg.pro(3, 3);
end;
/


--echo  # 写入s1的受限值, s1>2
--error 2290
begin
my_pkg.pro(-2, 3);
end;
/

--echo  # 写入s2的受限值, s2<20
--error 2290
begin
my_pkg.pro(5, 20);
end;
/

--echo  # 写入s1和s2的同时受限值, s1>2, s2<20
--error 2290
begin
my_pkg.pro(1, 30);
end;
/

--echo  # 写入s2的foreign key之外的值 2,3,4,20
--error 2290
begin
my_pkg.pro(5, 26);
end;
/

###--echo  # 验证primary key(s1,s2)
###--error 00001
###begin
###my_pkg.pro(3, 3);
###end;
###/

--echo  # stu1中验证s2字段的unique, 已有2,3,4,20
###--error 00001
###begin
###my_pkg.pro2(5, 2);
###end;
###/

--echo  # stu1中验证s1字段的primary key
###--error 00001
###begin
###my_pkg.pro2(20, 20);
###end;
###/

--echo  # stu1中验证s1字段check, 大于0
--error 2290
begin
my_pkg.pro2(-1, 21);
end;
/

--echo  # stu1中验证s2字段check, 大于0
--error 2290
begin
my_pkg.pro2(6, -6);
end;
/


--echo  # update stu2中的s1, s1>2
--error 2290
begin
my_pkg.pro4(-1,3);
end;
/

--echo  # update stu2中的s2, s2<20
--error 2290
begin
my_pkg.pro7(68, 2);
end;
/

--echo  # 删除stu1内容
begin
my_pkg.pro5('xxxxx');
end;
/

--echo  # 删除stu2内容
begin
my_pkg.pro6(99);
end;
/
delimiter ;/
--enable_result_log
--enable_query_log
--echo  ##################自定义类型create type...as...##################
--disable_warnings
--error 0,942
drop table stu;
--error 0,600
drop type my_type;
--enable_warnings


delimiter /;
--error 600,201
create or replace type my1_type as table of my_type;
/

delimiter ;/



--echo  ##################type record测试##################
delimiter /;
begin
declare
type ty_rd is record(s1 varchar2(4), s2 varchar2(4));
ty_rd1 ty_rd;
ty_rd2 ty_rd;
begin
ty_rd1.s1:=9527;
ty_rd1.s2:='abcd';
ty_rd2:=ty_rd1;
DBMS_output.put_line(ty_rd1.s1);
DBMS_output.put_line(ty_rd1.s2);
DBMS_output.put_line(ty_rd2.s1);
DBMS_output.put_line(ty_rd2.s2);
end;
end;
/

delimiter ;/

--echo  ##数据对象gb18030-3//gb18030-3-6
--echo  ##################procedure及匿名块中的使用##################
--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop type my_type;
--enable_warnings


create table stu(s1 varchar2(20), s2 varchar2(20));
insert into stu values('我1a', '我1a');
create table stu1(s1 varchar(20), s2 varchar2(20));


delimiter /;
create or replace procedure pro is
x varchar2(20);
y varchar2(20);
begin
select s1 into x from stu;
select s2 into y from stu;
insert into stu1 values(x,y);
end;
/

begin
pro();
end;
/

--echo  # 读取nchar类型的值, 分别写入varchar和varchar2类型中;OB报错12899，ORACLE报错OBE-06502: PL/SQL: numeric or value error: character string buffer too small
declare
x varchar2(20);
y varchar2(20);
begin
select s1 into x from stu;
select s2 into y from stu;
insert into stu1 values(x,y);
end;
/

delimiter ;/
select * from stu1;


--echo  ##################%type的使用##################
--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop type my_type;
--enable_warnings


create table stu(s0 varchar2(2), s1 varchar2(20), s2 varchar2(20));
insert into stu values(1, '我1a', '我1a');
insert into stu values(2, '123', '456');
create table stu1(s0 number(2), s1 varchar2(40), s2 varchar2(40));


delimiter /;
declare
x stu.s1%type;
y stu.s2%type;
begin
select s1,s2 into x,y from stu where s0=1;
insert into stu1 values(1,x||'中',y || 'a');
end;
/

declare
x stu.s1%type;
y stu.s2%type;
begin
select s1,s2 into x,y from stu where s0=2;
insert into stu1 values(2,x+10000000,y+100000);
end;
/

delimiter ;/
--echo  # varchar和varchar2内容写入nchar中
insert into stu1 values(3,'中中1','你你');

delimiter /;
declare
x stu.s1%type:=0;
y stu.s2%type:=0;
begin
select s1,s2 into x,y from stu1 where s0=2;
insert into stu values(3,x,y);
end;
/


###--echo  # 暂不测试
###--error 00600,6502
###declare
###x stu.s1%type:=1234567;
###y stu.s2%type:='abcdefg';
###begin
###insert into stu values(4,x,y);
###end;
###/

delimiter ;/



--echo  ##################%rowtype的使用##################
--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop type my_type;
--enable_warnings


create table stu(s1 varchar2(20), s2 varchar2(20));
insert into stu values(12, '上海');
create table stu1(s1 number(3), s2 varchar2(40));

delimiter /;
--echo  # 类型间的隐式转换
declare
x stu%rowtype;
y varchar(20):=0;
z varchar2(20):=0;
begin
select s1,s2 into y,z from stu;
insert into stu1 values(y+100, '中国' || z);
end;
/

declare
x stu%rowtype;
begin
x.s1:='123';
DBMS_output.put_line('输出结果: ' || x.s1);
end;
/

delimiter ;/

--echo  ##数据对象gb18030-3//gb18030-3-2
--echo  ##################函数中的nchar应用##################
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

--echo  # 创建表
create table stu(s1 varchar2(2), s2 number, s3 char(2), s4 varchar2(2), s5 varchar2(20) generated always as(s1*2));
insert into stu(s1,s2,s3,s4) values(1,1,1,1);
insert into stu(s1,s2,s3,s4) values(50,50,50,50);

--echo  # 函数中都nchar类型的增删改查
--echo  # 查看各类型的值
delimiter /;

--echo  # 
create or replace function mynum(x in varchar2)
return varchar2
is
y varchar2(1):=1;
begin
select s1 into y from stu;return y;
return y;
end;
/
delimiter ;/


--echo  ##数据对象gb18030-3//gb18030-3-1
--echo  ##################trigger中的应用##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu1;
--error 0,4080
drop trigger stu_tg;
--enable_warnings

--echo  # 创建表
create table stu(s1 varchar2(2) primary key, s2 varchar2(2) not null, s3 varchar2(2), check(length(s3)<5));

--echo  # 创建trigger, 验证insert和update超限字符
delimiter /;
create or replace trigger stu_tg before insert or update on stu
for each row
begin
if length(:new.s3)>=5 then
select 'abcd' into :new.s3 from dual;
else select :new.s3 into :new.s3 from dual;
end if;
end stu_tg;
/
delimiter ;/

--echo  # 触发器的触发,验证预期异常, insert和update超过2个字符的内容
insert into stu values(1,2,3);
--error 12899
insert into stu values(2,2,'abcd');
--error 12899
update stu set s3='abc' where s1=1;
delete from stu;


--echo  ##################trigger中的应用##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,4080
drop trigger stu_tg;
--enable_warnings

--echo  # 创建表
create table stu(s1 varchar2(20) primary key, s2 varchar2(20) not null, s3 varchar2(20) unique);

--echo  # 创建trigger
delimiter /;
create or replace trigger stu_tg before insert or update on stu
for each row
begin
if length(:new.s3)>=5 then
select 'abcd' into :new.s3 from dual;
else select :new.s3 into :new.s3 from dual;
end if;
end stu_tg;
/
delimiter ;/

--echo  # 触发器的触发,验证primary key,not null,unique及字符超限
insert into stu values(1,2,'a');
insert into stu values(2,2,1);
###--error 00001
###insert into stu values(2,2,3);
--error 01400
insert into stu values(3,'',4);
--error 01400
insert into stu values(4,null,5);
insert into stu values(5,5,6);
--disable_result_log
###--error 00001
###insert into stu values(6,6,6);
--enable_result_log
insert into stu values(7,6,'ab');
insert into stu values(8,6,'abaaaaaa');

--echo  # 创建trigger
delimiter /;
create or replace trigger stu_tg before insert or update on stu
for each row
begin
dbms_output.put_line('触发器的输出');
if length(:new.s3)<2 then
:new.s3:='abc';
else select :new.s3 into :new.s3 from dual;
end if;
end stu_tg;
/
delimiter ;/

insert into stu values(10,6,'ba');
insert into stu values(11,6,'b');


--echo  ##数据对象gb18030-3//gb18030-3-5
--echo  ##################sequence中的应用##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu1;
--error 0,2289
drop sequence seq;
--enable_warnings

--echo  # 创建表, 测试序列
create table stu(s1 varchar2(1) unique, s2 varchar2(1) not null, s3 varchar2(1) primary key);
create table stu1(s1 varchar2(1), s2 varchar2(1), s3 varchar2(1),s4 varchar2(1));
insert into stu1 values(1,1,1,1);

--echo  # 创建序列
create sequence seq
minvalue 1
nomaxvalue
start with 5
increment by 1
nocycle
nocache;

--echo  # 使用序列insert
insert into stu select seq.nextval, seq.nextval, seq.nextval from dual;
--error 01400
insert into stu select seq.nextval, '', seq.nextval from dual;
###select seq.nextval from stu;
--disable_result_log
--error 00001
insert into stu values(5,seq.nextval,seq.nextval);
--enable_result_log
###--error 00001
###insert into stu values(seq.nextval,seq.nextval,5);
###select seq.nextval from stu;
###--error 12899
###insert into stu select seq.nextval, seq.nextval, seq.nextval from dual;

--echo  # 添加虚拟列
alter table stu add ss varchar2(20) generated always as (s2+1);

--echo  # 虚拟列insert成功了
--error 600,54013
insert into stu select seq.nextval-5, seq.nextval-5, seq.nextval-5, seq.nextval from dual;
--error 600,54013
insert into stu select * from stu1;

--echo  # 修改虚拟列依赖
#--error 00600
#alter table stu modify ss as (s1+1);

###select * from stu;

--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu1;
--error 0,2289
drop sequence seq;
--enable_warnings

--disable_warnings
--error 0,942
DROP TABLE nn1;
--error 0,942
DROP TABLE nn2;
--error 0,942
DROP TABLE t1;
--enable_warnings


create table nn1(c1 int, c2 int, c3 varchar2(10));
create table nn2(d1 int, d2 int, d3 varchar2(10));
create table t1(a varchar2(100), b int);

delimiter /;

create or replace procedure insert_p_nn1nn2 is
begin
    insert into nn1 values(mod(20, 7), mod(20, 6), concat('hello1', mod(20, 3)));
    insert into nn2 values(mod(20, 5), mod(20, 3), concat('hello1', mod(20, 2)));
end;
/

delimiter ;/

call insert_p_nn1nn2();

--disable_warnings
--error 0,942
DROP TABLE nn1;
--error 0,942
DROP TABLE nn2;
--error 0,942
DROP TABLE t1;
--enable_warnings