--echo ##################################函数场景测试##################################
delimiter //;
--echo ######函数-一般赋值运算-字符表达式,NVARCHAR2######
create or replace function fun1
return NVARCHAR2
 is
v_char1 NVARCHAR2(20);
v_char3 NVARCHAR2(20);
v_char4 NVARCHAR2(20);
v_char5 NVARCHAR2(20);
v_char6 NVARCHAR2(20);
begin
v_char1:=ASCII('中');
v_char3:=LOWER('中文测试001a');
v_char4:=UPPER('中文测试001a');
v_char5:=LTRIM(' 中文测试001a');
v_char6:=RTRIM('中文测试001a ');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
return v_char1;
end;
//

delimiter ;//
select fun1 from dual;
--echo # 展示结果
select println() from dual;

delimiter //;
--echo ######函数-一般赋值运算-字符表达式,CLOB######
create or replace function fun111
return NVARCHAR2
 is
v_char1 NVARCHAR2(20);
v_char3 CLOB;
v_char4 CLOB;
v_char5 CLOB;
v_char6 CLOB;
begin
v_char1:='中';
v_char3:=LOWER('中文测试001a');
v_char4:=UPPER('中文测试001a');
v_char5:=LTRIM(' 中文测试001a');
v_char6:=RTRIM('中文测试001a ');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
return v_char1;
end;
//

delimiter ;//
select fun111 from dual;
--echo # 展示结果
select println() from dual;


delimiter //;
--echo ######函数-一般赋值运算-字符表达式,VARCHAR2######
create or replace function fun2
return VARCHAR2
 is
v_char1 VARCHAR2(20);
v_char3 VARCHAR2(20);
v_char4 VARCHAR2(20);
v_char5 VARCHAR2(20);
v_char6 VARCHAR2(20);
begin
v_char1:=ASCII('中');
v_char3:=LOWER('中文测试001a');
v_char4:=UPPER('中文测试001a');
v_char5:=LTRIM(' 中文测试001a');
v_char6:=RTRIM('中文测试001a ');
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
return v_char1;
end;
//

delimiter ;//
select fun2 from dual;
--echo # 展示结果
select println() from dual;

delimiter //;
--echo ######函数-一般赋值运算-字符表达式,VARCHAR2--生僻字---不识别######
create or replace function fun2
return VARCHAR2
 is
v_char1 VARCHAR2(200);
v_char3 VARCHAR2(200);
v_char4 VARCHAR2(200);
v_char5 VARCHAR2(200);
v_char6 VARCHAR2(200);
v_char7 VARCHAR2(200);
begin
v_char1:=ASCII('黒 匼');
v_char3:=LOWER('黒 匼');
v_char4:=UPPER('黒 匼');
v_char5:=LTRIM(' 黒 匼');
v_char6:=RTRIM('黒 匼');
v_char7:='黒 匼';
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
dbms_output.put_line(v_char7);
return v_char7;
end;
//

delimiter ;//
select fun2 from dual;
--echo # 展示结果
select println() from dual;


delimiter //;
--echo ######函数-一般赋值运算-字符表达式,VARCHAR2--其他字符######
create or replace function fun2
return VARCHAR2
 is
v_char1 VARCHAR2(200);
v_char3 VARCHAR2(200);
v_char4 VARCHAR2(200);
v_char5 VARCHAR2(2000);
v_char6 VARCHAR2(200);
v_char7 VARCHAR2(200);
v_char8 VARCHAR2(200);
v_char9 VARCHAR2(200);
v_char10 VARCHAR2(200);
v_char11 VARCHAR2(200);
v_char12 VARCHAR2(200);
v_char13 VARCHAR2(2000);
begin
v_char1:='?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??';
v_char3:='~@#￥%……&*（）——+：“》？《.+/';
v_char4:='中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国';
v_char5:='哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥';
v_char6:='“”';
v_char7:='中华人民共和国中华唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾'; 
v_char8:='AAAAAAAAAAAADDSGDFHGFJVGHKGHKGHLGLGLHJOLYSGSHBXsdfdsfdsgdsgdfhdfjgfmbvkyuolyhuj;lhjl.jbhn,.dfgdsgsgs'; 
v_char9:='関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。'; 
v_char10:='?? ???? ?? ??? ? ? ??? ???? ?? ?? ?? ? ??.'; 
v_char11:='苏州码子（〡、〢、〣、〤、〥、〦、〧、〨、〩、十）'; 
v_char12:='Ⅰ Ⅱ Ⅲ Ⅳ Ⅴ Ⅵ Ⅶ Ⅷ Ⅸ Ⅹ Ⅺ Ⅻ'; 
v_char13:='defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）'; 
dbms_output.put_line(v_char1);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
dbms_output.put_line(v_char7);
dbms_output.put_line(v_char8);
dbms_output.put_line(v_char9);
dbms_output.put_line(v_char10);
dbms_output.put_line(v_char11);
dbms_output.put_line(v_char12);
dbms_output.put_line(v_char13);
return v_char7;
end;
//

delimiter ;//
select fun2 from dual;
--echo # 展示结果
select println() from dual;


--echo # 函数-赋值语句-数据库select into赋值--逻辑表达式，关系表达式,VARCHAR2#
--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(id number PRIMARY KEY, s_name varchar2(1000));

--disable_warnings
--error 0,942
drop table cls;
--enable_warnings
create table cls(id number PRIMARY KEY, c_name varchar2(1000));



delimiter //;
declare
s_name varchar2(800);
i number:=1;
begin
 while i<11 loop
   s_name:='黒 匼defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）'||i;
   insert into student values(i,s_name);
   i:=i+1;
   end loop;
end;
//

declare
c_name varchar2(800);
i number:=1;
begin
 while i<11 loop
   c_name:='黒 匼関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。'||i;
   insert into cls values(i,c_name);
   i:=i+1;
   end loop;
end;
//

delimiter ;//
select * from student order by id;
select * from cls order by id;

--disable_warnings
--error 0,942
drop table student;
--error 0,942
drop table cls;
--enable_warnings




--echo # 函数-赋值语句-数据库select into赋值--逻辑表达式，关系表达式,VARCHAR2#
--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(id number PRIMARY KEY, s_name varchar2(200));

--disable_warnings
--error 0,942
drop table cls;
--enable_warnings
create table cls(id number PRIMARY KEY, c_name varchar2(200));



delimiter //;
declare
s_name varchar2(20);
i number:=1;
begin
 while i<11 loop
   s_name:='张'||i;
   insert into student values(i,s_name);
   i:=i+1;
   end loop;
end;
//

declare
c_name varchar2(20);
i number:=1;
begin
 while i<11 loop
   c_name:='班级'||i;
   insert into cls values(i,c_name);
   i:=i+1;
   end loop;
end;
//


create or replace function fun3
return VARCHAR2
 is
    v_name varchar2(20);
    c_name varchar2(20);
    v_id number;
    v_n1 number;
    v_n2 number;
    v_n3 number;
    v_n4 number;
    v_n5 number;
    --exists,between
    cursor v_students is select s_name  from student where  exists(select id  from student where id between 1 and 3 or s_name like '张9');
    --!=,or
    cursor v_students2 is select s_name from student where id!=1 or s_name='张9';
begin
    --逻辑运算<=，表名.字段名表达式
    select s_name into v_name from student where id <= 1;
    dbms_output.put_line(v_name);
    --逻辑运算>,all,like,*
    select * into v_id,v_name from student where id > all ( select id  from student where s_name like '张9%' );
    dbms_output.put_line(v_id);
    dbms_output.put_line(v_name);
    --逻辑运算=,and
    select s_name into v_name from student where id = 1 and s_name='张1';
    dbms_output.put_line(v_name);
    --逻辑运算>=,any,in
    select s_name into v_name from student where id >= any ( select id  from student where id in(select id from student where s_name='张10') );
    dbms_output.put_line(v_name);
    open v_students;
    loop
    exit when v_students%notfound;
    fetch v_students into v_name;
    dbms_output.put_line(v_name);
    end loop;
    close v_students;
    --some,<
    select s_name into v_name from student where id < some ( select id  from student where s_name like '张2%' );
    dbms_output.put_line(v_name);
    --not,<>
    select s_name into v_name from student where id not in(select id from student where s_name<>'张9');
    dbms_output.put_line(v_name);
    open v_students2;
    loop
    exit when v_students2%notfound;
    fetch v_students2 into v_name;
    dbms_output.put_line(v_name);
    end loop;
    close v_students2;
    --目标表达式  [<表名.>]<字段名表达式>[,[<表名>.]<字段名表达式>]
    select c.c_name,s.s_name into c_name,v_name from cls c,student s where c.id=1 and s.id=1; 
    --聚合函数
    select max(id),min(id),avg(id),sum(id),count(*) into v_n1,v_n2,v_n3,v_n4,v_n5 from student;
    dbms_output.put_line(v_n1);
    dbms_output.put_line(v_n2);
    dbms_output.put_line(v_n3);
    dbms_output.put_line(v_n4);
    dbms_output.put_line(v_n5);
    return c_name;
end;
//
delimiter ;//
select fun3 from dual;

--echo # 展示结果
select println() from dual;


--echo # 函数-赋值语句-数据库select into赋值--逻辑表达式，关系表达式NVARCHAR2#
--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(id number PRIMARY KEY, s_name nvarchar2(20));

--disable_warnings
--error 0,942
drop table cls;
--enable_warnings
create table cls(id number PRIMARY KEY, c_name nvarchar2(20));


delimiter //;
declare
s_name nvarchar2(20);
i number:=1;
begin
 while i<11 loop
   s_name:='张'||i;
   insert into student values(i,s_name);
   i:=i+1;
   end loop;
end;
//

declare
c_name nvarchar2(20);
i number:=1;
begin
 while i<11 loop
   c_name:='班级'||i;
   insert into cls values(i,c_name);
   i:=i+1;
   end loop;
end;
//


create or replace function fun3
return NVARCHAR2
 is
    v_name nvarchar2(20);
    c_name nvarchar2(20);
    v_id number;
    v_n1 number;
    v_n2 number;
    v_n3 number;
    v_n4 number;
    v_n5 number;
    --exists,between
    cursor v_students is select s_name  from student where  exists(select id  from student where id between 1 and 3 or s_name like '张9');
    --!=,or
    cursor v_students2 is select s_name from student where id!=1 or s_name='张9';
begin
    --逻辑运算<=，表名.字段名表达式
    select s_name into v_name from student where id <= 1;
    dbms_output.put_line(v_name);
    --逻辑运算>,all,like,*
    select * into v_id,v_name from student where id > all ( select id  from student where s_name like '张9%' );
    dbms_output.put_line(v_id);
    dbms_output.put_line(v_name);
    --逻辑运算=,and
    select s_name into v_name from student where id = 1 and s_name='张1';
    dbms_output.put_line(v_name);
    --逻辑运算>=,any,in
    select s_name into v_name from student where id >= any ( select id  from student where id in(select id from student where s_name='张10') );
    dbms_output.put_line(v_name);
    open v_students;
    loop
    exit when v_students%notfound;
    fetch v_students into v_name;
    dbms_output.put_line(v_name);
    end loop;
    close v_students;
    --some,<
    select s_name into v_name from student where id < some ( select id  from student where s_name like '张2%' );
    dbms_output.put_line(v_name);
    --not,<>
    select s_name into v_name from student where id not in(select id from student where s_name<>'张9');
    dbms_output.put_line(v_name);
    open v_students2;
    loop
    exit when v_students2%notfound;
    fetch v_students2 into v_name;
    dbms_output.put_line(v_name);
    end loop;
    close v_students2;
    --目标表达式  [<表名.>]<字段名表达式>[,[<表名>.]<字段名表达式>]
    select c.c_name,s.s_name into c_name,v_name from cls c,student s where c.id=1 and s.id=1; 
    --聚合函数
    select max(id),min(id),avg(id),sum(id),count(*) into v_n1,v_n2,v_n3,v_n4,v_n5 from student;
    dbms_output.put_line(v_n1);
    dbms_output.put_line(v_n2);
    dbms_output.put_line(v_n3);
    dbms_output.put_line(v_n4);
    dbms_output.put_line(v_n5);
    return c_name;
end;
//
delimiter ;//
select fun3 from dual;

--echo # 展示结果
select println() from dual;


##--echo # 函数-赋值语句-数据库select into赋值--逻辑表达式，关系表达式CLOB#
##--disable_warnings
##--error 0,942
##drop table student;
##--enable_warnings
##create table student(id number PRIMARY KEY, s_name CLOB);
##
##--disable_warnings
##--error 0,942
##drop table cls;
##--enable_warnings
##create table cls(id number PRIMARY KEY, c_name CLOB);
##
##
##delimiter //;
##declare
##s_name CLOB;
##i number:=1;
##begin
## while i<11 loop
##   s_name:='张'||i;
##   insert into student values(i,s_name);
##   i:=i+1;
##   end loop;
##end;
##//
##
##declare
##c_name CLOB;
##i number:=1;
##begin
## while i<11 loop
##   c_name:='班级'||i;
##   insert into cls values(i,c_name);
##   i:=i+1;
##   end loop;
##end;
##//
##
##
##create or replace function fun3
##return NVARCHAR2
## is
##    v_name CLOB;
##    c_name NVARCHAR2(200);
##    v_id number;
##    v_n1 number;
##    v_n2 number;
##    v_n3 number;
##    v_n4 number;
##    v_n5 number;
##    --exists,between
##    cursor v_students is select s_name  from student where  exists(select id  from student where id between 1 and 3 or s_name like '张9');
##    --!=,or
##    cursor v_students2 is select s_name from student where id!=1 or s_name='张9';
##begin
##    --逻辑运算<=，表名.字段名表达式
##    select s_name into v_name from student where id <= 1;
##    dbms_output.put_line(v_name);
##    --逻辑运算>,all,like,*
##    select * into v_id,v_name from student where id > all ( select id  from student where s_name like '张9%' );
##    dbms_output.put_line(v_id);
##    dbms_output.put_line(v_name);
##    --逻辑运算=,and
##    select s_name into v_name from student where id = 1 and s_name='张1';
##    dbms_output.put_line(v_name);
##    --逻辑运算>=,any,in
##    select s_name into v_name from student where id >= any ( select id  from student where id in(select id from student where s_name='张10') );
##    dbms_output.put_line(v_name);
##    open v_students;
##    loop
##    exit when v_students%notfound;
##    fetch v_students into v_name;
##    dbms_output.put_line(v_name);
##    end loop;
##    close v_students;
##    --some,<
##    select s_name into v_name from student where id < some ( select id  from student where s_name like '张2%' );
##    dbms_output.put_line(v_name);
##    --not,<>
##    select s_name into v_name from student where id not in(select id from student where s_name<>'张9');
##    dbms_output.put_line(v_name);
##    open v_students2;
##    loop
##    exit when v_students2%notfound;
##    fetch v_students2 into v_name;
##    dbms_output.put_line(v_name);
##    end loop;
##    close v_students2;
##    --目标表达式  [<表名.>]<字段名表达式>[,[<表名>.]<字段名表达式>]
##    select c.c_name,s.s_name into c_name,v_name from cls c,student s where c.id=1 and s.id=1; 
##    --聚合函数
##    select max(id),min(id),avg(id),sum(id),count(*) into v_n1,v_n2,v_n3,v_n4,v_n5 from student;
##    dbms_output.put_line(v_n1);
##    dbms_output.put_line(v_n2);
##    dbms_output.put_line(v_n3);
##    dbms_output.put_line(v_n4);
##    dbms_output.put_line(v_n5);
##    return c_name;
##end;
##//
##delimiter ;//
##--error 00932
##select fun3 from dual;
##
##--echo # 展示结果
##select println() from dual;
##
--echo # 函数-赋值语句-数据库select into赋值--正则表达式#
--disable_warnings
--error 0,942
drop table regtest;
--enable_warnings
create table regtest(id varchar(4),value varchar(100));
insert into regtest values('1','1234560');
insert into regtest values('4','abc');

delimiter //;
create or replace function fun5
return varchar2
is
v_replace varchar2(100);
v_replace1 varchar2(100);
v_replace2 varchar2(100);
v_replace4 nvarchar2(100);
v_replace5 nvarchar2(100);
v_replace6 nvarchar2(100);
v_replace7 clob;
v_replace8 clob;
v_replace9 clob;
v_replace10 varchar2(100);
v_replace11 varchar2(100);
v_replace12 varchar2(100);
v_replace13 varchar2(100);
v_replace14 nvarchar2(100);
v_replace15 clob;
v_like varchar2(100);
v_number1 number;
v_number2 number;
begin
    select value into v_like from regtest  where value like '1____60';
    dbms_output.put_line(v_like);
    select regexp_replace('0123456789','01234','0abc') into v_replace from dual;
    dbms_output.put_line(v_replace);
	select regexp_replace('中文中文1','中文','英文') into v_replace1 from dual;
    dbms_output.put_line(v_replace1);
	select regexp_replace('中文中文abc','中文','韩文') into v_replace2 from dual;
    dbms_output.put_line(v_replace2);
	select regexp_replace('0123456789','01234','0abc') into v_replace4 from dual;
    dbms_output.put_line(v_replace4);
	select regexp_replace('中文中文1','中文','英文') into v_replace5 from dual;
    dbms_output.put_line(v_replace5);
	select regexp_replace('中文中文abc','中文','韩文') into v_replace6 from dual;
    dbms_output.put_line(v_replace6);
	select regexp_replace('0123456789','01234','0abc') into v_replace7 from dual;
    dbms_output.put_line(v_replace7);
	select regexp_replace('中文中文1','中文','英文') into v_replace8 from dual;
    dbms_output.put_line(v_replace8);
	select regexp_replace('中文中文abc','中文','韩文') into v_replace9 from dual;
    dbms_output.put_line(v_replace9);	
	select REGEXP_COUNT('中文中文','中文',1,'i') into v_replace10 from dual;
    dbms_output.put_line(v_replace10);
	select REGEXP_COUNT('中文英文文aaa','中文',1,'i') into v_replace11 from dual;
    dbms_output.put_line(v_replace11);
	select REGEXP_COUNT('中文英文文1中文英文问','中文',1,'i') into v_replace12 from dual;
    dbms_output.put_line(v_replace12);
	select value into v_replace13 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace13);
	select value into v_replace14 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace14);
	select value into v_replace15 from regtest  where regexp_like(value,'^[0-9]|[-]$');
    dbms_output.put_line(v_replace15);
    select REGEXP_INSTR ('hello itmyhome', 'e') into v_number1 FROM dual; 
    dbms_output.put_line(v_number1);
    select REGEXP_SUBSTR('17,20,23','[^,]+',1,1,'i') into v_number2 from DUAL;
    dbms_output.put_line(v_number2); 
    return v_replace;
end;
//

delimiter ;//
###OB有报错OBE-00600: internal error code, arguments: -4258, Incorrect string
###select fun5 from dual;
###
###--echo # 展示结果
###select println() from dual;

--echo # 函数-赋值语句-数据库select into,普通赋值,字符串函数#
delimiter //;
CREATE OR REPLACE FUNCTION fun9
            return varchar2
            IS
v_char1 varchar2(100);
v_char2 varchar2(100);
v_char3 varchar2(100);
v_char4 varchar2(100);
v_char5 varchar2(100);
v_char6 varchar2(100);
v_char7 varchar2(100);
v_char8 varchar2(100);
v_char9 varchar2(100);
v_char10 varchar2(100);
v_char11 varchar2(100);
v_char12 varchar2(100);
v_char13 varchar2(200);
v_char14 varchar2(100);
v_char15 varchar2(100);
v_char16 varchar2(100);
v_char17 varchar2(100);
v_char18 varchar2(100);
v_number1 number;
v_number2 number;
begin
  v_char1:=trim(' 中文 ');
  v_char2:=ltrim(' 中文 ');
  v_char3:=rtrim(' 中文 ');
  v_char4:=ASCII('中');
  v_char5:=LOWER('英文');
  v_char6:=UPPER('英文');
  v_char7:=substr('你好呀',2,4);
  v_char9:=REPLACE('可以可以','可','以'); 
  v_char8:=chr(65);
  v_number1:=instr('你好呀 你好呀', '好', -1, 1);
  select decode(sign(100-90),-1,100,90) into v_number2 from dual;
  select NVL('中文测试','asad') ,lpad('中文',10,'中'),rpad('中文',5,'文'),TRANSLATE('PI*R*2','*R','#@') into v_char10,v_char15,v_char16,v_char17 from dual;
  select NVL2(null,'好','的'),concat('中','文'),INITCAP('中 文'),length('中文的长度') into v_char11,v_char12,v_char13,v_char14 from dual; 
  dbms_output.put_line(v_char1);
dbms_output.put_line(v_char2);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
dbms_output.put_line(v_char7);
dbms_output.put_line(v_char8);
dbms_output.put_line(v_char9);
dbms_output.put_line(v_char10);
dbms_output.put_line(v_char11);
dbms_output.put_line(v_char12);
dbms_output.put_line(v_char13);
dbms_output.put_line(v_char14);
dbms_output.put_line(v_char15);
dbms_output.put_line(v_char16);
dbms_output.put_line(v_char17);
dbms_output.put_line(v_char18);
dbms_output.put_line(v_number1);
dbms_output.put_line(v_number2);
return v_char1;
end;
//
delimiter ;//
select fun9 from dual;

--echo # 展示结果
select println() from dual;

--echo # 函数-赋值语句-数据库select into,普通赋值,字符串函数#
delimiter //;
CREATE OR REPLACE FUNCTION fun9
            return nvarchar2
            IS
v_char1 nvarchar2(100);
v_char2 nvarchar2(100);
v_char3 nvarchar2(100);
v_char4 nvarchar2(100);
v_char5 nvarchar2(100);
v_char6 nvarchar2(100);
v_char7 nvarchar2(100);
v_char8 nvarchar2(100);
v_char9 nvarchar2(100);
v_char10 nvarchar2(100);
v_char11 nvarchar2(100);
v_char12 nvarchar2(100);
v_char13 nvarchar2(200);
v_char14 nvarchar2(100);
v_char15 nvarchar2(100);
v_char16 nvarchar2(100);
v_char17 nvarchar2(100);
v_char18 nvarchar2(100);
v_number1 number;
v_number2 number;
begin
  v_char1:=trim(' 中文 ');
  v_char2:=ltrim(' 中文 ');
  v_char3:=rtrim(' 中文 ');
  v_char4:=ASCII('中');
  v_char5:=LOWER('英文');
  v_char6:=UPPER('英文');
  v_char7:=substr('你好呀',2,4);
  v_char9:=REPLACE('可以可以','可','以'); 
  v_char8:=chr(65);
  v_number1:=instr('你好呀 你好呀', '好', -1, 1);
  select decode(sign(100-90),-1,100,90) into v_number2 from dual;
  select NVL('中文测试','asad') ,lpad('中文',10,'中'),rpad('中文',5,'文'),TRANSLATE('PI*R*2','*R','#@') into v_char10,v_char15,v_char16,v_char17 from dual;
  select NVL2(null,'好','的'),concat('中','文'),INITCAP('中 文'),length('中文的长度') into v_char11,v_char12,v_char13,v_char14 from dual; 
  dbms_output.put_line(v_char1);
dbms_output.put_line(v_char2);
dbms_output.put_line(v_char3);
dbms_output.put_line(v_char4);
dbms_output.put_line(v_char5);
dbms_output.put_line(v_char6);
dbms_output.put_line(v_char7);
dbms_output.put_line(v_char8);
dbms_output.put_line(v_char9);
dbms_output.put_line(v_char10);
dbms_output.put_line(v_char11);
dbms_output.put_line(v_char12);
dbms_output.put_line(v_char13);
dbms_output.put_line(v_char14);
dbms_output.put_line(v_char15);
dbms_output.put_line(v_char16);
dbms_output.put_line(v_char17);
dbms_output.put_line(v_char18);
dbms_output.put_line(v_number1);
dbms_output.put_line(v_number2);
return v_char1;
end;
//
delimiter ;//
select fun9 from dual;

--echo # 展示结果
select println() from dual;


###OB有报错600: 'internal error code, arguments: -4002, Invalid argument', instead of 932...
###--error 00932
###select fun9 from dual;
###
###--echo # 展示结果
###select println() from dual;


--echo # 函数-条件选择语句IF分支,case分支--集合方法#
delimiter //;
CREATE OR REPLACE FUNCTION fun21
            return varchar2
            IS
type ind is table of varchar2(200);
    v ind;
begin
  v:=ind('中','文','测','试'); ---初始化同时可以赋初始值
  --if else
  if v.first()=1 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
   if v.last()=4 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
  if v.count=4 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
  if v.limit=null then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
  if v.next(2)=3 then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
  if v.prior(2)=1 then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
  if v.EXISTS(4) then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
  --if elsif else
   if v.first()=1 then
    dbms_output.put_line('true');
   elsif v.last()=4 then
    dbms_output.put_line('true');
  elsif v.count=4 then
    dbms_output.put_line('true');
  elsif v.limit=null then
     dbms_output.put_line('true');
  elsif v.next(2)=3 then
     dbms_output.put_line('true');
  --elsif v.prior(2)=1 then
  --   dbms_output.put_line('true');
  elsif v.EXISTS(4) then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
  --case表达式 when
   case v.first()
     when 1 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
   case v.last()
     when 4 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.count
    when 4 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.next(2)
    when 3 then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.prior(2)
    when 1 then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.EXISTS(4) 
    when true then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  --case when表达式
   case when v.first()=1 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
   case when v.last()=4 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case when v.count=4 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case when v.limit=null then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case when v.next(2)=3 then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case when v.prior(2)=1 then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case when v.EXISTS(4)=true then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  --case表达式when表达式
  case v.first()
     when v.first() then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
   case v.last()
     when v.last() then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.count
    when v.count then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.next(2)
    when v.next(2) then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.prior(2)
    when v.prior(2) then
     dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  case v.EXISTS(4) 
    when v.EXISTS(4) then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end case;
  return '中';
end;
//

select fun21 from dual;
//


--echo # 展示结果
select println() from dual;//

delimiter ;//

--echo # 函数-赋值语句--批量绑定--select into 中使用bulk collect--在fetch into中使用bulk collect --varchar2 #
--disable_warnings
--error 0,942
drop table test_forall;
--error 0,942
drop table mag_user_basic;
--enable_warnings
create table test_forall ( user_id number(10), user_name varchar2(20));
create table mag_user_basic ( user_id number(10), user_name varchar2(20));
insert into mag_user_basic values(1,'张三');
insert into mag_user_basic values(2,'李四');
insert into mag_user_basic values(3,'王五');

delimiter //;
create or replace function fun34
return number
 is
TYPE table_forall IS TABLE OF test_forall%ROWTYPE;  
  v_table table_forall;
  CURSOR c1 IS  
    SELECT mub.user_id,mub.user_name FROM mag_user_basic mub; 
pragma autonomous_transaction;
begin
SELECT mub.user_id,mub.user_name 
         BULK COLLECT INTO v_table
    FROM mag_user_basic mub;
    FORALL idx IN 1..v_table.COUNT
           INSERT INTO test_forall VALUES v_table(idx);
    COMMIT; 
    OPEN c1;  
   
   FETCH c1 BULK COLLECT INTO v_table;  
   FORALL idx IN 1..v_table.COUNT  
         INSERT INTO test_forall VALUES v_table(idx);  
    COMMIT;
return 23;
end;
//

declare x varchar2(4000) := fun34(); begin dbms_output.put_line(x); end;//
delimiter ;//

--echo # 展示结果
select println() from dual;


--echo # 函数-赋值语句--批量绑定--在returning into中使用bulk collect#
--disable_warnings
--error 0,942
drop table test_forall2;
--enable_warnings
CREATE TABLE test_forall2 AS SELECT * FROM test_forall;

delimiter //;
create or replace function fun35
return number
is
   TYPE IdList IS TABLE OF test_forall.User_Id%TYPE;
   enums IdList;
   TYPE NameList IS TABLE OF test_forall.user_name%TYPE;
   names NameList; 
   pragma autonomous_transaction;
BEGIN
   DELETE FROM test_forall2 WHERE user_id = 1
        RETURNING user_id, user_name BULK COLLECT INTO enums, names; 
   dbms_output.put_line('Deleted ' || SQL%ROWCOUNT || ' rows:');
   FOR i IN enums.FIRST .. enums.LAST
   LOOP
     dbms_output.put_line('User #' || enums(i) || ': ' || names(i));
   END LOOP;
   COMMIT; 
return 35;   
END;
//

declare x varchar2(4000) := fun35(); begin dbms_output.put_line(x); end;//
delimiter ;//


--echo # 展示结果
select println() from dual;


--echo # 函数-赋值语句--批量绑定--select into 中使用bulk collect--在fetch into中使用bulk collect --nvarchar2 #
--disable_warnings
--error 0,942
drop table test_forall;
--error 0,942
drop table mag_user_basic;
--enable_warnings
create table test_forall ( user_id number(10), user_name nvarchar2(20));
create table mag_user_basic ( user_id number(10), user_name nvarchar2(20));
insert into mag_user_basic values(1,'张三');
insert into mag_user_basic values(2,'李四');
insert into mag_user_basic values(3,'王五');

delimiter //;
create or replace function fun34
return number
 is
TYPE table_forall IS TABLE OF test_forall%ROWTYPE;  
  v_table table_forall;
  CURSOR c1 IS  
    SELECT mub.user_id,mub.user_name FROM mag_user_basic mub; 
begin
SELECT mub.user_id,mub.user_name 
         BULK COLLECT INTO v_table
    FROM mag_user_basic mub;
    FORALL idx IN 1..v_table.COUNT
           INSERT INTO test_forall VALUES v_table(idx);
    COMMIT; 
    OPEN c1;  
   
   FETCH c1 BULK COLLECT INTO v_table;  
   FORALL idx IN 1..v_table.COUNT  
         INSERT INTO test_forall VALUES v_table(idx);  
    COMMIT;
return 23;
end;
//

declare x varchar2(4000) := fun34(); begin dbms_output.put_line(x); end;//
delimiter ;//

--echo # 展示结果
select println() from dual;


--echo # 函数-赋值语句--批量绑定--在returning into中使用bulk collect#
--disable_warnings
--error 0,942
drop table test_forall2;
--enable_warnings
CREATE TABLE test_forall2 AS SELECT * FROM test_forall;

delimiter //;
create or replace function fun35
return number
is
   TYPE IdList IS TABLE OF test_forall.User_Id%TYPE;
   enums IdList;
   TYPE NameList IS TABLE OF test_forall.user_name%TYPE;
   names NameList; 
BEGIN
   DELETE FROM test_forall2 WHERE user_id = 1
        RETURNING user_id, user_name BULK COLLECT INTO enums, names; 
   dbms_output.put_line('Deleted ' || SQL%ROWCOUNT || ' rows:');
   FOR i IN enums.FIRST .. enums.LAST
   LOOP
     dbms_output.put_line('User #' || enums(i) || ': ' || names(i));
   END LOOP;
   COMMIT; 
return 35;   
END;
//

declare x varchar2(4000) := fun35(); begin dbms_output.put_line(x); end;//
delimiter ;//

--echo # 展示结果
select println() from dual;

--echo # 函数-赋值语句--批量绑定--select into 中使用bulk collect--在fetch into中使用bulk collect -- clob #
--disable_warnings
--error 0,942
drop table test_forall;
--error 0,942
drop table mag_user_basic;
--enable_warnings
create table test_forall ( user_id number(10), user_name clob);
create table mag_user_basic ( user_id number(10), user_name clob);
insert into mag_user_basic values(1,'张三');
insert into mag_user_basic values(2,'李四');
insert into mag_user_basic values(3,'王五');

delimiter //;
create or replace function fun34
return number
 is
TYPE table_forall IS TABLE OF test_forall%ROWTYPE;  
  v_table table_forall;
  CURSOR c1 IS  
    SELECT mub.user_id,mub.user_name FROM mag_user_basic mub; 
begin
SELECT mub.user_id,mub.user_name 
         BULK COLLECT INTO v_table
    FROM mag_user_basic mub;
    FORALL idx IN 1..v_table.COUNT
           INSERT INTO test_forall VALUES v_table(idx);
    COMMIT; 
    OPEN c1;  
   
   FETCH c1 BULK COLLECT INTO v_table;  
   FORALL idx IN 1..v_table.COUNT  
         INSERT INTO test_forall VALUES v_table(idx);  
    COMMIT;
return 23;
end;
//

declare x varchar2(4000) := fun34(); begin dbms_output.put_line(x); end;//
delimiter ;//

--echo # 展示结果
select println() from dual;

--echo # 函数-赋值语句--批量绑定--在returning into中使用bulk collect#
--disable_warnings
--error 0,942
drop table test_forall2;
--enable_warnings
CREATE TABLE test_forall2 AS SELECT * FROM test_forall;

delimiter //;
create or replace function fun35
return number
is
   TYPE IdList IS TABLE OF test_forall.User_Id%TYPE;
   enums IdList;
   TYPE NameList IS TABLE OF test_forall.user_name%TYPE;
   names NameList; 
BEGIN
   DELETE FROM test_forall2 WHERE user_id = 1
        RETURNING user_id, user_name BULK COLLECT INTO enums, names; 
   dbms_output.put_line('Deleted ' || SQL%ROWCOUNT || ' rows:');
   FOR i IN enums.FIRST .. enums.LAST
   LOOP
     dbms_output.put_line('User #' || enums(i) || ': ' || names(i));
   END LOOP;
   COMMIT; 
return 35;   
END;
//

declare x varchar2(4000) := fun35(); begin dbms_output.put_line(x); end;//
delimiter ;//

--echo # 展示结果
select println() from dual;

--disable_warnings
--error 0,942
drop table regtest;
--error 0,942
drop table student;
--error 0,942
drop table cls;
--enable_warnings

delimiter //;
delimiter ;//
