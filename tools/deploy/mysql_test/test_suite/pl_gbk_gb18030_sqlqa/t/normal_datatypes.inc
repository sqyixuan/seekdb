--echo ##################################各种数据类型场景测试##################################
--echo ######oracle复合数据类型 record(pl/sql记录,相当于高级语言中的结构)用法示例  varchar2######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname varchar2(2000),stuage varchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_record_type is record--is是必须的，不能替换为as
(
name student.stuname%type,
age student.stuage%type
);
stu_record stu_record_type;
begin
select stuname,stuage into stu_record from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;

--echo ######oracle复合数据类型 record(pl/sql记录,相当于高级语言中的结构)用法示例  nvarchar2######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid nvarchar2(20), stuname nvarchar2(2000),stuage nvarchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_record_type is record--is是必须的，不能替换为as
(
name student.stuname%type,
age student.stuage%type
);
stu_record stu_record_type;
begin
select stuname,stuage into stu_record from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;


--echo ######oracle复合数据类型 record(pl/sql记录,相当于高级语言中的结构)用法示例  nchar######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid nchar(20), stuname nchar(1000),stuage nchar(1000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_record_type is record--is是必须的，不能替换为as
(
name student.stuname%type,
age student.stuage%type
);
stu_record stu_record_type;
begin
select stuname,stuage into stu_record from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;



--echo ######oracle复合数据类型 record(pl/sql记录,相当于高级语言中的结构)用法示例  clob######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname clob,stuage clob);
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_record_type is record--is是必须的，不能替换为as
(
name student.stuname%type,
age student.stuage%type
);
stu_record stu_record_type;
begin
select stuname,stuage into stu_record from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
select stuname,stuage into stu_record from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_record.name||' 年龄为:'||stu_record.age);
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;




--echo ######table(pl/sql表,不能作为表的列类型,相当于高级语言的数组，但没有大小限制，索引下标也没有界限)用法示例  varchar2######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname varchar2(2000),stuage varchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_table_type is table of student.stuname%type index by binary_integer;--定义一个数组类型
stu_table stu_table_type;--定义数组类型的变量
begin
select stuname into stu_table(-200) from student where stuid='001';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-200));
select stuname into stu_table(-201) from student where stuid='002';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-201));
select stuname into stu_table(-202) from student where stuid='003';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-202));
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;



--echo ######table(pl/sql表,不能作为表的列类型,相当于高级语言的数组，但没有大小限制，索引下标也没有界限)用法示例  nvarchar2######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname nvarchar2(2000),stuage nvarchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_table_type is table of student.stuname%type index by binary_integer;--定义一个数组类型
stu_table stu_table_type;--定义数组类型的变量
begin
select stuname into stu_table(-200) from student where stuid='001';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-200));
select stuname into stu_table(-201) from student where stuid='002';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-201));
select stuname into stu_table(-202) from student where stuid='003';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-202));
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;


--echo ######table(pl/sql表,不能作为表的列类型,相当于高级语言的数组，但没有大小限制，索引下标也没有界限)用法示例  nchar######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname nchar(1000),stuage nchar(1000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_table_type is table of student.stuname%type index by binary_integer;--定义一个数组类型
stu_table stu_table_type;--定义数组类型的变量
begin
select stuname into stu_table(-200) from student where stuid='001';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-200));
select stuname into stu_table(-201) from student where stuid='002';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-201));
select stuname into stu_table(-202) from student where stuid='003';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-202));
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;


--echo ######table(pl/sql表,不能作为表的列类型,相当于高级语言的数组，但没有大小限制，索引下标也没有界限)用法示例  clob######

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname clob,stuage clob);
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;

declare
type stu_table_type is table of student.stuname%type index by binary_integer;--定义一个数组类型
stu_table stu_table_type;--定义数组类型的变量
begin
select stuname into stu_table(-200) from student where stuid='001';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-200));
select stuname into stu_table(-201) from student where stuid='002';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-201));
select stuname into stu_table(-202) from student where stuid='003';--下标可以随意指定
dbms_output.put_line('姓名为:'||stu_table(-202));
end;
//

delimiter ;//

--echo # 展示结果
select println() from dual;

###OB暂不支持此语法
###--echo ######pl/sql嵌套表(相当于数组里的元素为对象，所以叫嵌套;可以作为表列，但必须指定存储表信息的数据表),用法示例:varchar2######
###delimiter //;
#####首先定义了一个对象，再定义一个嵌套表(一个数组(table)，数组元素为对象)
###CREATE OR REPLACE TYPE kingsql_type AS OBJECT(
###projectid NUMBER ,
###projectname VARCHAR2(500),
###projectfunds NUMBER,
###pubdate DATE
###);
###//
#####定义嵌套表类型——project_nested
###CREATE OR REPLACE TYPE kingsql_nested AS TABLE OF kingsql_type NOT NULL;
###//
###CREATE TABLE department_01(
###did NUMBER ,
###deptname VARCHAR2(500) NOT NULL,
###qt_column kingsql_nested
###)NESTED TABLE qt_column STORE AS kingsql_nested_t1((
###projectid NOT NULL,
###projectname NOT NULL,
###projectfunds NOT NULL,
###pubdate NOT NULL));
###//
###嵌套表插入数据
###insert into department_01 values(
###1,--第一列
###'黒 匼',--第二列
###kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));//
######第三列第一行/第三列第二行
######插入数据
###declare
###v1 kingsql_nested:=kingsql_nested(kingsql_type(2,'黒 匼 2',2,sysdate));
###begin
###insert into department_01
###values(1,'hehe1',kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));
###insert into department_01 values(2,'黒 匼 22',v1);
###end;
###//
###查询嵌套表数据
###select * from the(select department_01.qt_column from department_01 where did=1);//
###
###
###--echo ######pl/sql嵌套表(相当于数组里的元素为对象，所以叫嵌套;可以作为表列，但必须指定存储表信息的数据表),用法示例:nvarchar2######
###delimiter //;
#####首先定义了一个对象，再定义一个嵌套表(一个数组(table)，数组元素为对象)
###Create or replace type kingsql_type as object(
###Projectid number,
###Projectname nvarchar2(500),
###Projectfunds number,
###Pubdate date
###);
###//
#####定义嵌套表类型——project_nested
###CREATE OR REPLACE TYPE kingsql_nested AS TABLE OF kingsql_type NOT NULL ;//
###CREATE TABLE department_01 (
###did NUMBER ,
###deptname nvarchar2(500) NOT NULL ,
###qt_column kingsql_nested
###) NESTED TABLE qt_column STORE AS kingsql_nested_t1((
###projectid NOT NULL ,
###projectname NOT NULL ,
###projectfunds NOT NULL ,
###pubdate NOT NULL)) ;//
###嵌套表插入数据
###insert into department_01 values(
###1,--第一列
###'黒 匼 1',--第二列
###kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));//
######第三列第一行/第三列第二行
######插入数据
###declare
###v1 kingsql_nested:=kingsql_nested(kingsql_type(2,'黒 匼 2',2,sysdate));
###begin
###insert into department_01
###values(1,'hehe1',kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));
###insert into department_01 values(2,'黒 匼 22',v1);
###end;
###//
###查询嵌套表数据
###select * from the(select department_01.qt_column from department_01 where did=1);//
###
###--echo ######pl/sql嵌套表(相当于数组里的元素为对象，所以叫嵌套;可以作为表列，但必须指定存储表信息的数据表),用法示例:nchar######
###delimiter //;
#####首先定义了一个对象，再定义一个嵌套表(一个数组(table)，数组元素为对象)
###Create or replace type kingsql_type as object(
###Projectid number,
###Projectname nchar(500),
###Projectfunds number,
###Pubdate date
###);
###//
#####定义嵌套表类型——project_nested
###CREATE OR REPLACE TYPE kingsql_nested AS TABLE OF kingsql_type NOT NULL ;//
###CREATE TABLE department_01 (
###did NUMBER ,
###deptname nchar(500) NOT NULL ,
###qt_column kingsql_nested
###) NESTED TABLE qt_column STORE AS kingsql_nested_t1((
###projectid NOT NULL ,
###projectname NOT NULL ,
###projectfunds NOT NULL ,
###pubdate NOT NULL)) ;//
###嵌套表插入数据
###insert into department_01 values(
###1,--第一列
###'黒 匼 1',--第二列
###kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));//
######第三列第一行/第三列第二行
######插入数据
###declare
###v1 kingsql_nested:=kingsql_nested(kingsql_type(2,'黒 匼 2',2,sysdate));
###begin
###insert into department_01
###values(1,'hehe1',kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));
###insert into department_01 values(2,'黒 匼 22',v1);
###end;
###//
###查询嵌套表数据
###select * from the(select department_01.qt_column from department_01 where did=1);//
###
###
###--echo ######pl/sql嵌套表(相当于数组里的元素为对象，所以叫嵌套;可以作为表列，但必须指定存储表信息的数据表),用法示例:clob######
#####首先定义了一个对象，再定义一个嵌套表(一个数组(table)，数组元素为对象)
###Create or replace type kingsql_type as object(
###Projectid number,
###Projectname clob,
###Projectfunds number,
###Pubdate date
###);
###//
#####定义嵌套表类型——project_nested
###CREATE OR REPLACE TYPE kingsql_nested AS TABLE OF kingsql_type NOT NULL ;//
###CREATE TABLE department_01 (
###did NUMBER ,
###deptname clob NOT NULL ,
###qt_column kingsql_nested
###) NESTED TABLE qt_column STORE AS kingsql_nested_t1((
###projectid NOT NULL ,
###projectname NOT NULL ,
###projectfunds NOT NULL ,
###pubdate NOT NULL)) ;//
###嵌套表插入数据
###insert into department_01 values(
###1,--第一列
###'黒 匼 1',--第二列
###kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));//
######第三列第一行/第三列第二行
######插入数据
###declare
###v1 kingsql_nested:=kingsql_nested(kingsql_type(2,'黒 匼 2',2,sysdate));
###begin
###insert into department_01
###values(1,'hehe1',kingsql_nested(kingsql_type(1,'黒 匼 1',1,sysdate),kingsql_type(11,'黒 匼 11',11,sysdate)));
###insert into department_01 values(2,'黒 匼 22',v1);
###end;
###//
###查询嵌套表数据
###select * from the(select department_01.qt_column from department_01 where did=1);//

###OB语法暂不支持，可以创建，但表中使用有语法报错900
##--echo ######varray(固定长度的嵌套表,可直接作为表列使用，不需要额外的表来存储),用法示例varchar2######
##delimiter //;
##create or replace type article_type as object(
##title varchar2(30), 
##pubdate date);
##//
##create or replace type article_array is array(20) of article_type;
##//
##
##--disable_warnings
##--error 0,942
##drop table author;
##--enable_warnings
#####再用varary来定义数据列:
##delimiter ;//
##create table author(
##id number(6),name varchar2(10),article article_array);
##
##insert into author values (1,'zhangsan',article_array(article_type('黒 匼 1',sysdate),article_type('黒 匼 2',sysdate)));
##select * from author;
##
##--echo ######varray(固定长度的嵌套表,可直接作为表列使用，不需要额外的表来存储),用法示例nvarchar2######
##delimiter //;
##create type article_type as object(
##title nvarchar2(30), 
##pubdate date);
##//
##create or replace type article_array is array(20) of article_type;
##//
#####再用varary来定义数据列:
##delimiter ;//
##create table author(
##id number(6),name nvarchar2(10),article article_array);
##
##insert into author values (1,'zhangsan',article_array(article_type('黒 匼 1',sysdate),article_type('黒 匼 2',sysdate)));
##select * from author;
##
##--echo ######varray(固定长度的嵌套表,可直接作为表列使用，不需要额外的表来存储),用法示例nchar######
##delimiter //;
##create type article_type as object(
##title nchar(30), 
##pubdate date);
##//
##create or replace type article_array is array(20) of article_type;
##//
#####再用varary来定义数据列:
##delimiter ;//
##create table author(
##id number(6),name nchar(10),article article_array);
##
##insert into author values (1,'zhangsan',article_array(article_type('黒 匼 1',sysdate),article_type('黒 匼 2',sysdate)));
##select * from author;
##
##--echo ######varray(固定长度的嵌套表,可直接作为表列使用，不需要额外的表来存储),用法示例clob######
##delimiter //;
##create type article_type as object(
##title clob, 
##pubdate date);
##//
##create or replace type article_array is array(20) of article_type;
##//
#####再用varary来定义数据列:
##delimiter ;//
##create table author(
##id number(6),name clob,article article_array);
##
##insert into author values (1,'zhangsan',article_array(article_type('黒 匼 1',sysdate),article_type('黒 匼 2',sysdate)));
##select * from author;


--echo ##################################引用类型##################################
--echo ######ref-cursor(创建动态游标，定义游标时不指定sql,open时再定义sql),用法示例######

--disable_warnings
--error 0,942
drop table DEPT;
--error 0,942
drop table EMP;
--enable_warnings


CREATE TABLE DEPT
(DEPTNO NUMBER(2) PRIMARY KEY,
DNAME NVARCHAR2(14),
LOC VARCHAR2(13));
CREATE TABLE EMP(EMPNO NUMBER(4) PRIMARY KEY,
ENAME NVARCHAR2(100),
JOB NVARCHAR2(9),
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
(7839,'黒 匼1','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO EMP VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO EMP VALUES
(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20);
INSERT INTO EMP VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO EMP VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7934,'黒 匼2','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);


delimiter //;
DECLARE 
TYPE c1 IS REF CURSOR; 
emp_cursor c1; 
v_ename emp.ename%TYPE;
 v_sal emp.sal%TYPE; 
BEGIN 
OPEN emp_cursor FOR SELECT ename,sal from emp where deptno=10;
 LOOP fetch emp_cursor into v_ename,v_sal; 
exit when emp_cursor%notfound; 
dbms_output.put_line(v_ename);
end loop;
close emp_cursor; 
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;

##不支持在表中使用
##--echo ######ref obj_type(对象指针)，用法示例######
##
#####首先定义一个对象，再用这个对象创建一个表，表的每一行都是一个对象，再插入数据:
##
##delimiter //;
##create or replace type home_type as object(
##street varchar2(500),
##city varchar2(200),
##state varchar2(200),
##zipcode varchar2(600),
##owner varchar2(1000)
##);//
##create table homes of home_type;//
##insert into homes values('呼和浩特11号','黒 匼 2','状态','?????????????▲???÷↑↓◆◇⊙■□△▽?','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能');
#####再创建一个表person，其中addr列类型为对象home_type(对象指针)，当插入这个列时，只插入一个实体的引用即可:
##create table person(
##id number(6) primary key,
##name varchar2(10), addr REF home_type);//
##
##Insert into person select 1,'马鸣',ref(p) from homes p where p.street = '呼和浩特11号';//
##
##select * from person;//
##
##delimiter ;//


--echo ##################################属性类型##################################

--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname varchar2(2000),stuage varchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;
declare
stu_name student.stuname%type;
begin
select stuname into stu_name from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_name);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;

delimiter //;
declare
stu_row student%rowtype;--定义一个表示数据表一行的变量
begin
select * into stu_row from student where stuid='001';--此处必须为*，否则抛出异常(值过多)
dbms_output.put_line('姓名为:'||stu_row.stuname);
select * into stu_row from student where stuid='002';--此处必须为*，否则抛出异常(值过多)
dbms_output.put_line('姓名为:'||stu_row.stuname);
select * into stu_row from student where stuid='003';--此处必须为*，否则抛出异常(值过多)
dbms_output.put_line('姓名为:'||stu_row.stuname);
select * into stu_row from student where stuid='004';--此处必须为*，否则抛出异常(值过多)
dbms_output.put_line('姓名为:'||stu_row.stuname);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;



--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname nvarchar2(2000),stuage varchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;
declare
stu_name student.stuname%type;
begin
select stuname into stu_name from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_name);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;

delimiter //;
declare
stu_row student%rowtype;--定义一个表示数据表一行的变量
begin
select * into stu_row from student where stuid='001';--此处必须为*，否则抛出异常(值过多)
dbms_output.put_line('姓名为:'||stu_row.stuname);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname nchar(1000),stuage varchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;
declare
stu_name student.stuname%type;
begin
select stuname into stu_name from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_name);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;

delimiter //;
declare
stu_row student%rowtype;--定义一个表示数据表一行的变量
begin
select * into stu_row from student where stuid='001';--此处必须为*，否则抛出异常(值过多)
dbms_output.put_line('姓名为:'||stu_row.stuname);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;


--disable_warnings
--error 0,942
drop table student;
--enable_warnings
create table student(stuid varchar2(20), stuname clob,stuage varchar2(2000));
insert into student values ('001','张三','十八');
insert into student values ('002','黒 匼','黒 匼');
insert into student values ('003','哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥','関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。');
insert into student values ('004','?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??','defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）');

delimiter //;
declare
stu_name student.stuname%type;
begin
select stuname into stu_name from student where stuid='001';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='002';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='003';
dbms_output.put_line('姓名为:'||stu_name);
select stuname into stu_name from student where stuid='004';
dbms_output.put_line('姓名为:'||stu_name);
end;
//

delimiter ;//
--echo # 展示结果
select println() from dual;

delimiter //;
declare
stu_row student%rowtype;--定义一个表示数据表一行的变量
begin
select * into stu_row from student where stuid='001';--此处必须为*，否则抛出异常(值过多)
dbms_output.put_line('姓名为:'||stu_row.stuname);
end;
//
delimiter ;//
--echo # 展示结果
select println() from dual;


--echo ##################################PL/SQL用户定义的子类型##################################
delimiter //;
DECLARE
   SUBTYPE name IS char(20);
   SUBTYPE message IS varchar2(1000);
   salutation name;
   greetings message;
BEGIN
   salutation := '黒 匼';
   greetings := '黒 匼 哎 哀 唉 埃 挨 锿 捱 皑  関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。?????????○??◎●???????????⊙??defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（ ';
   dbms_output.put_line('Hello ' || salutation || greetings);
END;
//
delimiter ;//
--echo # 展示结果
select println() from dual;

delimiter //;
DECLARE
   SUBTYPE name IS nchar(20);
   SUBTYPE message IS nvarchar2(1000);
   salutation name;
   greetings message;
BEGIN
   salutation := '黒 匼';
   greetings := '黒 匼 哎 哀 唉 埃 挨 锿 捱 皑  関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。?????????○??◎●???????????⊙??defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（ ';
   dbms_output.put_line('Hello ' || salutation || greetings);
END;
//
delimiter ;//
--echo # 展示结果
select println() from dual;

delimiter //;
DECLARE
   SUBTYPE name IS char(20);
   SUBTYPE message IS nchar(1000);
   salutation name;
   greetings message;
BEGIN
   salutation := '黒 匼';
   greetings := '黒 匼 哎 哀 唉 埃 挨 锿 捱 皑  関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。?????????○??◎●???????????⊙??defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（ ';
   dbms_output.put_line('Hello ' || salutation || greetings);
END;
//
delimiter ;//
--echo # 展示结果
select println() from dual;

delimiter //;
DECLARE
   SUBTYPE name IS clob;
   SUBTYPE message IS clob;
   salutation name;
   greetings message;
BEGIN
   salutation := '黒 匼';
   greetings := '黒 匼 哎 哀 唉 埃 挨 锿 捱 皑  関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。?????????○??◎●???????????⊙??defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（ ';
   dbms_output.put_line('Hello ' || salutation || greetings);
END;
//
delimiter ;//
--echo # 展示结果
select println() from dual;