connection conn_oracle_obtest;

ob1.oblog.gen_conf tb_white_list=oracle.*.* log_level=DEBUG;
ob1.oblog.restart;

################# all_pl_ddl ##################

## 创建procedure
create or replace procedure proc is begin null; end;
ob1.oblog.next;

## 删除procedure
drop procedure proc;
ob1.oblog.next;

## 创建function
create or replace function func return int is begin return 1; end;
ob1.oblog.next;

## 删除function
drop function func;
ob1.oblog.next;

## 创建package
create or replace package pack is x int; y char(1); procedure proc; end;
ob1.oblog.next;

## 删除package
drop package pack;
ob1.oblog.next;

## 创建package body
create or replace package body pack is procedure proc is begin null; end; end;
ob1.oblog.next;

## 删除package
drop package body pack;
ob1.oblog.next;

## 创建package
create or replace package pack is x int; y char(1); procedure proc; end;
ob1.oblog.next;

## 创建package body
create or replace package body pack is procedure proc is begin null; end; end;
ob1.oblog.next;

## 删除package
drop package pack;
ob1.oblog.next;

## 创建udt
create or replace type ty is table of int;
ob1.oblog.next;

## 删除udt
drop type ty;
ob1.oblog.next;

## 创建udt
create or replace type ty is object(col1 int, col2 int);
ob1.oblog.next;

## 删除udt
drop type ty;
ob1.oblog.next;

## 创建表
create table t(col int);
ob1.oblog.next;

## 匿名块中执行dml
begin insert into t values(1); insert into t values(2); update t set col = 3 where col = 1; end;
ob1.oblog.next;

## 匿名块中执行dml, SELECT语句不会产生日志, 不需要调用oblog.next
declare type t is table of int; x t; begin select col bulk collect into x from t; end;

## prcedure中执行dml
create or replace procedure proc is begin delete from t; end;
ob1.oblog.next;

## call
call proc();
ob1.oblog.next;

## 删除procedure
drop procedure proc;
ob1.oblog.next;

## package中的procedure执行dml
create or replace package pack is procedure proc; end;
ob1.oblog.next;

create or replace package body pack is procedure proc is begin delete from t; end; end;
ob1.oblog.next;

## 在表中插入一行数据
insert into t values(1);
ob1.oblog.next;

call pack.proc();
ob1.oblog.next;

drop package pack;
ob1.oblog.next;

## 匿名块中执行ddl
begin execute immediate 'drop table t;'; end;
ob1.oblog.next;

## 匿名块中执行ddl,dml
begin execute immediate 'create table t(col int)'; execute immediate 'insert into t values(1)'; end;
ob1.oblog.next;
ob1.oblog.next;

## 删除表
drop table t;
ob1.oblog.next;

################## basic dml ##################
create user oblog_db identified by "passwd";
# 输出创建user DDL
ob1.oblog.next;

# alter session不会生成redo日志，不需要执行liboblog输出数据
alter session set current_schema = oblog_db;

create table basic_1(c0 int primary key, c1 number, c2 varchar(10));
ob1.oblog.next;

insert into basic_1 values (1, 1.1, 'aaaa');
select c0, c1, c2 from basic_1;
ob1.oblog.next;

insert into basic_1 values (2, 2.22, 'bbbbbbbbbb');
select c0, c1, c2 from basic_1;
ob1.oblog.next;

update basic_1 set c1=2.2 where c0=2;
select c0, c1, c2 from basic_1;
ob1.oblog.next;

delete from basic_1;
select c0, c1, c2 from basic_1;
ob1.oblog.next;


ob1.oblog.stop;
