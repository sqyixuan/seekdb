

--disable_query_log
--echo ##底层对象创建
--echo ##表，视图，序列，子程序等
create table c1(t1 blob default hextoraw('a'),t2 varchar2(20) not null,t3 raw(20),t4 clob,t5 date,t6 timestamp,t7 number,t8 binary_float,t9 clob,t10 interval day to second);
--error 00900
create table c2(t1 boolean);

--echo ###创建索引,约束uinque,not null，check
create index c1_index on c1(t3,t5);
alter table c1 add constraint c1_constraint1 unique (t7);
alter table c1 add constraint c1_constraint check (t6 is not null and t8>-10);
--echo ##分区（range，list，hash，subpartition）
create table c3(t1 number) partition by range (t1)(
  partition part_1 values less than (10),
  partition part_2 values less than (20));
create table c4(t1 number) partition by list (t1)(
  partition part_1 values (1,10),
  partition part_2 values (11,20));
create table c5(t1 number) partition by hash (t1) PARTITIONS 2;
create table c6(t1 number,t2 number) partition by range(t1) subpartition by hash(t2) subpartitions 3 (
partition part_1 values less than (10),
partition part_2 values less than (20));

--echo ##视图
create view c3_view as select * from c3;
create view c1_view as select * from c1;

--echo ##序列
create sequence c_seq increment by 1 start with 1;

--echo ##同义词，临时表
create table c7(t1 number);
create synonym c7_syn for c7;
create global temporary table c8(t1 number);

delimiter /;
--echo ##type object/table + 多重嵌套
create or replace type cai_tp1 is table of number;
/
create or replace type cai_tp2 force as object(a number);
/
create or replace type cai_tp3 is table of cai_tp2;
/
create or replace type cai_tp4 is table of anydata;
/
create or replace type cai_tp5 as object(a cai_tp3);
/

--echo ##子程序（过程，function等+多重嵌套）
create or replace procedure cai_pro(a varchar,b varchar default null) is
 procedure c_pro(c varchar,d varchar) is
 begin
  execute immediate c||d;
 end;
begin
c_pro(a,b);
end;
/
create or replace function cai_func(a varchar,b varchar default null) return number is
 function c_func(c varchar,d varchar) return number is
 begin
  execute immediate c||d;
 return 1;
 end;
begin
return c_func(a,b);
end;
/

create or replace package cai_pack is
procedure pack_pro(a varchar,b varchar default null);
function pack_func(a varchar,b varchar default null) return number;
end;
/
create or replace package body cai_pack is
procedure pack_pro(a varchar,b varchar default null) is
begin
cai_pro(a,b);
end;

function pack_func(a varchar,b varchar default null) return number is
begin
return cai_func(a,b);
end;
end;
/

create or replace type cai_type as object(
a varchar(2000),
b varchar(2000),
static procedure type_pro(a varchar,b varchar),
member function type_func return number
);
/
create or replace type body cai_type is
static procedure type_pro(a varchar,b varchar) is
begin
cai_pack.pack_pro(a,b);
end;

member function type_func return number is
begin
return cai_pack.pack_func(a,b);
end;
end;
/
#declare
#b varchar(2000):='insert into c2 values(1)';
#a cai_type:=cai_type(b,null);
#begin
#cai_type.type_pro(b,null);
#dbms_output.put_line('输出type_func: '||a.type_func);
#end;
#/

create table c11(t1 number);/
create table c12(t1 number);/
create or replace trigger cai_trig
before insert on c12
for each row
declare
b varchar(2000):='insert into c11 values(1)';
ct cai_type:=cai_type(b,null);
begin
cai_type.type_pro(b,null);
dbms_output.put_line('输出type_func: '||ct.type_func);
end;
/
#insert into c12 values(1);/
#select * from c11;/
#select * from c12;/

--echo ##子程序内部逻辑为错误逻辑 + 为空逻辑
create or replace procedure cai_pro1 is
begin
null;
end;
/
create or replace function cai_func1 return number is
begin
return 'a';
end;
/

delimiter ;/


--enable_query_log