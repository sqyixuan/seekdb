#owner: webber.wb
#owner group: 技术部-SQL-存储过程
#description: OB-oracle到OB-oracle PL dblink 测试
#tags: pl
#author:whh01047778

--source mysql_test/test_suite/ob_ob_dblink_oracle_pl_sqlqa/include/output_print_enable.inc

--echo #-----------------------------------------------------------------------------
--echo # 远程OB端 dblink_tenant1租户下pl环境准备
--echo #-----------------------------------------------------------------------------

#case1: 存储过程-数据类型
--error 0,00942
drop table ori_tab1;
CREATE TABLE ori_tab1 (id int,c1 varchar2(4000) DEFAULT '备注1', c2 varchar2(4000) DEFAULT '备注2');
INSERT INTO ori_tab1 VALUES(1,DEFAULT,DEFAULT);

delimiter /;
create or replace procedure ori_pro1(
  p1 in varchar2 default 'remote varchar2',
  p2 in char default 'remote char',
  p3 in int default 11,
  p4 in number default 12,
  p5 in long default 'long',
  p6 in float default 13.456,
  p7 in binary_float default 14.567,
  p8 in binary_double default 15.789,
  p9 date default TO_DATE('2023-01-01', 'YYYY-MM-DD'),
  p10 timestamp default TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
  p11 timestamp with time zone default TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
  p12 timestamp with local time zone default TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) is
begin
  INSERT INTO ori_tab1 VALUES(2,'字符 in',p1||p2||p5);
  INSERT INTO ori_tab1 VALUES(3,'数字 in',p3||p4||p6||p7||p8);
  INSERT INTO ori_tab1 VALUES(4,'日期 in',p9||p10||p11||p12);
end;
/
create or replace procedure ori_pro2(
  p1 in out varchar2,
  p2 in out char,
  p3 in out int,
  p4 in out number,
  p5 in out long,
  p6 in out float,
  p7 in out binary_float,
  p8 in out binary_double,
  p9 in out date,
  p10 in out timestamp,
  p11 in out timestamp with time zone,
  p12 in out timestamp with local time zone) is
begin
  p1 := 'remote varchar2';
  p2 := 'remote char';
  p3 := 10 + p3;
  p4 := 11 + p4;
  p5 := 'remote long';
  p6 := 12.123 + p6;
  p7 := 13.456 + p7;
  p8 := 14.789 + p8;
  p9 := TO_DATE('2023-02-01', 'YYYY-MM-DD');
  p10 := TO_TIMESTAMP('2023-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p11 := TO_TIMESTAMP('2023-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p12 := TO_TIMESTAMP('2023-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  INSERT INTO ori_tab1 VALUES(5,'字符 in out',p1||p2||p5);
  INSERT INTO ori_tab1 VALUES(6,'数字 in out',p3||p4||p6||p7||p8);
  INSERT INTO ori_tab1 VALUES(7,'日期 in out',p9||p10||p11||p12);
end;
/
create or replace procedure ori_pro3(
  p1 out varchar2,
  p2 out char,
  p3 out int,
  p4 out number,
  p5 out long,
  p6 out float,
  p7 out binary_float,
  p8 out binary_double,
  p9 out date,
  p10 out timestamp,
  p11 out timestamp with time zone,
  p12 out timestamp with local time zone) is
begin
    p1 := 'remote varchar2';
    p2 := 'remote char';
    p3 := 10;
    p4 := 11;
    p5 := 'remote long';
    p6 := 12.123;
    p7 := 13.456;
    p8 := 14.789;
    p9 := TO_DATE('2023-02-01', 'YYYY-MM-DD');
    p10 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
    p11 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
    p12 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  INSERT INTO ori_tab1 VALUES(8,'字符 out',p1||p2||p5);
  INSERT INTO ori_tab1 VALUES(9,'数字 out',p3||p4||p6||p7||p8);
  INSERT INTO ori_tab1 VALUES(10,'日期 out',p9||p10||p11||p12);
end;
/
delimiter ;/

#case2: 函数-数据类型
--error 0,00942
drop table ori_tab2;
CREATE TABLE ori_tab2 (id int,c1 varchar2(4000) DEFAULT '备注1', c2 varchar2(4000) DEFAULT '备注2');
INSERT INTO ori_tab2 VALUES(1,DEFAULT,DEFAULT);

delimiter /;
create or replace function ori_fun1(
  p1 in varchar2 default 'remote varchar2',
  p2 in char default 'remote char',
  p3 in int default 11,
  p4 in number default 12,
  p5 in long default 'long',
  p6 in float default 13.456,
  p7 in binary_float default 14.567,
  p8 in binary_double default 15.789,
  p9 date default TO_DATE('2023-01-01', 'YYYY-MM-DD'),
  p10 timestamp default TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
  p11 timestamp with time zone default TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
  p12 timestamp with local time zone default TO_TIMESTAMP('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) 
  return varchar2 is 
begin
  INSERT INTO ori_tab2 VALUES(2,'字符 in',p1||p2||p5);
  INSERT INTO ori_tab2 VALUES(3,'数字 in',p3||p4||p6||p7||p8);
  INSERT INTO ori_tab2 VALUES(4,'日期 in',p9||p10||p11||p12);
  return 'ori_fun1 in';
end;
/
create or replace function ori_fun2(
  p1 in out varchar2,
  p2 in out char,
  p3 in out int,
  p4 in out number,
  p5 in out long,
  p6 in out float,
  p7 in out binary_float,
  p8 in out binary_double,
  p9 in out date,
  p10 in out timestamp,
  p11 in out timestamp with time zone,
  p12 in out timestamp with local time zone)
  return varchar2 is 
begin
  p1 := 'remote varchar2';
  p2 := 'remote char';
  p3 := 10 + p3;
  p4 := 11 + p4;
  p5 := 'remote long';
  p6 := 12.123 + p6;
  p7 := 13.456 + p7;
  p8 := 14.789 + p8;
  p9 := TO_DATE('2023-02-01', 'YYYY-MM-DD');
  p10 := TO_TIMESTAMP('2023-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p11 := TO_TIMESTAMP('2023-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p12 := TO_TIMESTAMP('2023-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  INSERT INTO ori_tab2 VALUES(5,'字符 in out',p1||p2||p5);
  INSERT INTO ori_tab2 VALUES(6,'数字 in out',p3||p4||p6||p7||p8);
  INSERT INTO ori_tab2 VALUES(7,'日期 in out',p9||p10||p11||p12);
  return 'ori_fun2 in out';
end;
/
create or replace function ori_fun3(
  p1 out varchar2,
  p2 out char,
  p3 out int,
  p4 out number,
  p5 out long,
  p6 out float,
  p7 out binary_float,
  p8 out binary_double,
  p9 out date,
  p10 out timestamp,
  p11 out timestamp with time zone,
  p12 out timestamp with local time zone) 
  return varchar2 is 
begin
  p1 := 'remote varchar2';
  p2 := 'remote char';
  p3 := 10;
  p4 := 11;
  p5 := 'remote long';
  p6 := 12.123;
  p7 := 13.456;
  p8 := 14.789;
  p9 := TO_DATE('2023-02-01', 'YYYY-MM-DD');
  p10 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p11 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p12 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  INSERT INTO ori_tab2 VALUES(8,'字符 out',p1||p2||p5);
  INSERT INTO ori_tab2 VALUES(9,'数字 out',p3||p4||p6||p7||p8);
  INSERT INTO ori_tab2 VALUES(10,'日期 out',p9||p10||p11||p12);
  return 'ori_fun3 out';
end;
/
delimiter ;/

#case3: 参数格式-边界值测试
delimiter /;
create or replace procedure ori_pro4(
  p1 out varchar2,
  p2 out char,
  p3 out int,
  p4 out number,
  p5 out long,
  p6 out float,
  p7 out binary_float,
  p8 out binary_double,
  p9 out date,
  p10 out timestamp,
  p11 out timestamp with time zone,
  p12 out timestamp with local time zone,
  p13 out varchar2,
  p14 out char,
  p15 out int,
  p16 out number,
  p17 out long,
  p18 out float,
  p19 out binary_float,
  p20 out binary_double,
  p21 out date,
  p22 out timestamp,
  p23 out timestamp with time zone,
  p24 out timestamp with local time zone,
  p25 out varchar2,
  p26 out char,
  p27 out int,
  p28 out number,
  p29 out long,
  p30 out float,
  p31 out binary_float,
  p32 out binary_double,
  p33 out date,
  p34 out timestamp,
  p35 out timestamp with time zone,
  p36 out timestamp with local time zone) is
begin
  p1 := 'remote varchar2';
  p2 := 'remote char';
  p3 := 10;
  p4 := 11;
  p5 := 'remote long';
  p6 := 12.123;
  p7 := 13.456;
  p8 := 14.789;
  p9 := TO_DATE('2023-02-01', 'YYYY-MM-DD');
  p10 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p11 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p12 := TO_TIMESTAMP('2023-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
  p13 := '';
  p14 := '';
  p15 := NULL;
  p16 := NULL;
  p17 := '';
  p18 := NULL;
  p19 := NULL;
  p20 := NULL;
  p21 := NULL;
  p22 := NULL;
  p23 := NULL;
  p24 := NULL;
  p25 := p1;
  p26 := p2;
  p27 := p3;
  p28 := p4;
  p29 := p5;
  p30 := p6;
  p31 := p7;
  p32 := p8;
  p33 := p9;
  p34 := p10;
  p35 := p11;
  p36 := p12;  
end;
/
delimiter ;/

#case4: 嵌套调用
--error 0,00942
drop table ori_tab3;
CREATE TABLE ori_tab3 (id int,c1 varchar2(4000) DEFAULT '备注1', c2 number DEFAULT 0);
INSERT INTO ori_tab1 VALUES(1,DEFAULT,DEFAULT);

delimiter /;
create or replace procedure ori_pro5(p1 in number default 10) is
begin
  INSERT INTO ori_tab3 VALUES(2,'ori_pro5',p1*2);
end;
/
create or replace procedure ori_pro6(p1 number default 20) is
begin
  ori_pro5(p1=>p1);
end;
/
create or replace procedure ori_pro7(p1 number default 30) is
begin
  ori_pro6(p1=>p1);
end;
/
create or replace function ori_fun8(p1 number default 40) return number is 
begin
  return p1*2;
end;
/
create or replace function ori_fun9(p1 number default 50) return number is 
begin
  return ori_fun8(p1=>p1*3);
end;
/
create or replace function ori_fun10(p1 number default 60) return number is 
begin
  return ori_fun9(p1=>p1*4);
end;
/
create or replace package ori_pck11 is
  a NUMBER default 70;
  procedure pack_pro(p1 in number default 80);
  function pack_func(p1 in number default 90) return number;
end;
/
create or replace package body ori_pck11 is
  procedure pack_pro(p1 in number default 80) is
  begin
    ori_pro7(p1=>p1);
    INSERT INTO ori_tab3 VALUES(2,'loc_pro5',p1);
  end;
  function pack_func(p1 in number default 90) return number is
  begin
    a := ori_fun10(p1=>p1);
    return a;
  end;
end;
/

#case5: 同义词场景
create or replace function ori_fun12(p1 number default 1) return number is 
begin
  return p1*2;
end;
/
delimiter ;/

create or replace synonym syn_ori_fun12 for ori_fun12;
create or replace synonym syn2_ori_fun12 for syn_ori_fun12;

delimiter /;
create or replace procedure ori_pro13(p1 in out number) is
begin
  p1 := syn2_ori_fun12(p1=>p1);
end;
/
delimiter ;/

create or replace synonym syn_ori_pro13 for ori_pro13;
create or replace synonym syn2_ori_pro13 for syn_ori_pro13;


#case6: 多库dblink访问
--error 0,00942
drop table ori_tab4;
CREATE TABLE ori_tab4 (id int,c1 varchar2(4000) DEFAULT '远程OB端1 dblink_tenant1', c2 number DEFAULT 0);
INSERT INTO ori_tab4 VALUES(1,DEFAULT,DEFAULT);

delimiter /;
create or replace procedure ori_pro14(p1 number default 20) is
begin
  INSERT INTO ori_tab4 VALUES(2,'ori_pro14',p1*2);
end;
/
create or replace function ori_fun15(p1 number default 1) return number is 
begin
  return power(p1,2);
end;
/
delimiter ;/

create or replace synonym syn_ori_pro14 for ori_pro14;
create or replace synonym syn_ori_fun15 for ori_fun15;

#case7: 正交原生oracle
--error 0,00942
drop table ob_t1;
create table ob_t1 (id char(16), data int);
insert into ob_t1 values ('远程OB端', 1);

delimiter /;
create or replace function ob_fun1 return number is
begin
  return 2;
end;
/
create or replace function ob_fun2(p1 number default 2) return number is
begin
  return ob_fun1 * p1;
end;
/

create or replace procedure ob_pro1(p1 number default 2) is
begin
  insert into ob_t1 values ('ob_pro1', p1);
end;
/
create or replace procedure ob_pro2(p1 number default 2) is
begin
  ob_pro1(p1=>p1);
end;
/

CREATE OR REPLACE PACKAGE ob_pkg1 IS
  FUNCTION func_30(p_char IN OUT CHAR) RETURN NUMBER;
END ob_pkg1;
/
CREATE OR REPLACE PACKAGE BODY ob_pkg1 IS
  FUNCTION func_30(p_char IN OUT CHAR) RETURN NUMBER IS
  BEGIN
    p_char := 'ob Modified Char';
    RETURN ob_fun1;
  END func_30;
END ob_pkg1;
/
delimiter ;/

create or replace synonym syn_ob_fun2 for ob_fun2;
create or replace synonym syn_ob_pro2 for ob_pro2;
create or replace synonym syn_ob_pkg1 for ob_pkg1;


#case8: dblink短连接




