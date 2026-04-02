
--echo ##################################存储过程场景测试##################################
delimiter //;
--echo ######存储过程-一般赋值运算-字符表达式,NVARCHAR2######
create or replace procedure 中文存储过程1
 is
中文列1 NVARCHAR2(20);
中文列3 NVARCHAR2(20);
中文列4 NVARCHAR2(20);
中文列5 NVARCHAR2(20);
中文列6 NVARCHAR2(20);
begin
中文列1:=ASCII('中');
中文列3:=LOWER('中文测试001a');
中文列4:=UPPER('中文测试001a');
中文列5:=LTRIM(' 中文测试001a');
中文列6:=RTRIM('中文测试001a ');
dbms_output.put_line(中文列1);
dbms_output.put_line(中文列3);
dbms_output.put_line(中文列4);
dbms_output.put_line(中文列5);
dbms_output.put_line(中文列6);
end;
//

delimiter ;//
call 中文存储过程1();
--echo # 展示结果
select println() from dual;

delimiter //;
--echo ######存储过程-一般赋值运算-字符表达式,CLOB######
create or replace procedure 中文存储过程111
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
end;
//

delimiter ;//
call 中文存储过程111();
--echo # 展示结果
select println() from dual;


delimiter //;
--echo ######存储过程-一般赋值运算-字符表达式,VARCHAR2######
create or replace procedure procedure2
 is
中文列1 VARCHAR2(20);
中文列3 VARCHAR2(20);
中文列4 VARCHAR2(20);
中文列5 VARCHAR2(20);
中文列6 VARCHAR2(20);
begin
中文列1:=ASCII('中');
中文列3:=LOWER('中文测试001a');
中文列4:=UPPER('中文测试001a');
中文列5:=LTRIM(' 中文测试001a');
中文列6:=RTRIM('中文测试001a ');
dbms_output.put_line(中文列1);
dbms_output.put_line(中文列3);
dbms_output.put_line(中文列4);
dbms_output.put_line(中文列5);
dbms_output.put_line(中文列6);
end;
//

delimiter ;//
call procedure2();
--echo # 展示结果
select println() from dual;

##delimiter //;
##--echo ######存储过程-一般赋值运算-字符表达式,VARCHAR2--生僻字---不识别######
##--error 0,600
##create or replace procedure procedure2
##is
##黒 匼 VARCHAR2(200);
##v_char3 VARCHAR2(200);
##begin
##黒 匼:=ASCII('黒 匼');
##v_char3:=LOWER('黒 匼');
##dbms_output.put_line(黒 匼);
##dbms_output.put_line(v_char3);
##end;
##//
##
##delimiter ;//
##call procedure2();
##--echo # 展示结果
##select println() from dual;


delimiter //;
--echo ######存储过程-一般赋值运算-字符表达式,VARCHAR2--其他字符######
--error 0,900
create or replace procedure ????????????
 is
v_char1 VARCHAR2(200);
begin
v_char1:='?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??';
dbms_output.put_line(v_char1);
end;
//

create or replace procedure 関数内部には問い合わせ文を書くことはできないがselectにより代入可能である
is
v_char1 VARCHAR2(200);
begin
v_char1:='関数内部には問い合わせ文を書くことはできないがselectにより代入可能である';
dbms_output.put_line(v_char1);
end;
//

delimiter ;//
###select 関数内部には問い合わせ文を書くことはできないがselectにより代入可能である from dual;
--echo # 展示结果
###select println() from dual;

--error 0,600
drop procedure 関数内部には問い合わせ文を書くことはできないがselectにより代入可能である;

delimiter //;
create or replace procedure defedtgtghgfjfhjf股份附加费苏州码子放到民共和国中华唉埃挨锿捱
 is
v_char1 VARCHAR2(200);
begin
v_char1:='defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）';
dbms_output.put_line(v_char1);
end;
//

delimiter ;//
--echo # 存储过程-赋值语句-数据库select into赋值--逻辑表达式，关系表达式,VARCHAR2#
--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(id number PRIMARY KEY, s_name varchar2(2000));

--disable_warnings
--error 0,942
drop table cls;
--enable_warnings
create table cls(id number PRIMARY KEY, c_name varchar2(2000));


delimiter //;
declare
中文变量 varchar2(800);
i number:=1;
begin
 while i<11 loop
   中文变量:='defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）'||i;
   insert into student values(i,中文变量);
   i:=i+1;
   end loop;
end;
//

declare
中文变量2 varchar2(800);
i number:=1;
begin
 while i<11 loop
   中文变量2:='関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。'||i;
   insert into cls values(i,中文变量2);
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


--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(id number PRIMARY KEY, s_name nvarchar2(2000));

--disable_warnings
--error 0,942
drop table cls;
--enable_warnings
create table cls(id number PRIMARY KEY, c_name nvarchar2(2000));

delimiter //;
--error 0,600
create or replace procedure procedure3
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
    cursor 游标stu is select s_name  from student where  exists(select id  from student where id between 1 and 3 or s_name like '张9');
    --!=,or
    cursor 游标stu2 is select s_name from student where id!=1 or s_name='张9';
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
    open 游标stu;
    loop
    exit when 游标stu%notfound;
    fetch 游标stu into v_name;
    dbms_output.put_line(v_name);
    end loop;
    close 游标stu;
    --some,<
    select s_name into v_name from student where id < some ( select id  from student where s_name like '张2%' );
    dbms_output.put_line(v_name);
    --not,<>
    select s_name into v_name from student where id not in(select id from student where s_name<>'张9');
    dbms_output.put_line(v_name);
    open 游标stu2;
    loop
    exit when 游标stu2%notfound;
    fetch 游标stu2 into v_name;
    dbms_output.put_line(v_name);
    end loop;
    close 游标stu2;
    --目标表达式  [<表名.>]<字段名表达式>[,[<表名>.]<字段名表达式>]
    select c.c_name,s.s_name into c_name,v_name from cls c,student s where c.id=1 and s.id=1; 
    --聚合存储过程
    select max(id),min(id),avg(id),sum(id),count(*) into v_n1,v_n2,v_n3,v_n4,v_n5 from student;
    dbms_output.put_line(v_n1);
    dbms_output.put_line(v_n2);
    dbms_output.put_line(v_n3);
    dbms_output.put_line(v_n4);
    dbms_output.put_line(v_n5);
end;
//
delimiter ;//
call procedure3();

--echo # 展示结果
select println() from dual;


delimiter //;
--echo # 存储过程-条件选择语句IF分支,case分支--集合方法#
CREATE OR REPLACE procedure procedure21
            IS
type ind is table of varchar2(200);
    集合v ind;
begin
  集合v:=ind('中','文','测','试'); ---初始化同时可以赋初始值
  --if else
  if 集合v.first()=1 then
    dbms_output.put_line('true');
  else
    dbms_output.put_line('false');
  end if;
end;
//

delimiter ;//
call procedure21();

--echo # 展示结果
select println() from dual;


--echo # 存储过程-赋值语句--批量绑定--select into 中使用bulk collect--在fetch into中使用bulk collect --varchar2 #
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
create or replace procedure procedure34
 is
TYPE 表类型table_forall IS TABLE OF test_forall%ROWTYPE;  
  v_table 表类型table_forall;
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
end;
//

delimiter ;//
call procedure34();

--echo # 展示结果
select println() from dual;


--echo # 存储过程-赋值语句--批量绑定--在returning into中使用bulk collect#
--disable_warnings
--error 0,942
drop table test_forall2;
--enable_warnings
CREATE TABLE test_forall2 AS SELECT * FROM test_forall;

delimiter //;
create or replace procedure procedure35
is
   TYPE ID类型IdList IS TABLE OF test_forall.User_Id%TYPE;
   enums ID类型IdList;
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
END;
//

delimiter ;//
call procedure35();


--echo # 展示结果
select println() from dual;


delimiter //;

--echo  ##################procedure存储过程中的应用in/out参数 ##################
create or replace procedure MyProcedure(标志 in INTEGER)    --param1形式参数，能接受实际参数传来的值
AS
BEGIN
    DBMS_OUTPUT.PUT_LINE(标志);    --param1为null
END;
//

delimiter ;//
call MyProcedure(10);


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

