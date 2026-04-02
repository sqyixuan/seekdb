#owner: webber.wb
#owner group: 技术部-SQL-存储过程
#description: OB-oracle到OB-oracle PL dblink 测试
#tags: pl
#author:whh01047778

--source mysql_test/test_suite/ob_ob_dblink_oracle_pl_sqlqa/include/output_print_enable.inc

--echo #-----------------------------------------------------------------------------
--echo # 远程OB端 dblink_tenant1租户下测试环境准备
--echo #-----------------------------------------------------------------------------
-- echo # 表
--error 0,00942
drop table ori_tab1;
CREATE TABLE ori_tab1 (id int, c1 char(2000) DEFAULT 'ori_tab1',c2 varchar2(4000) DEFAULT '备注');
INSERT INTO ori_tab1 VALUES(1,DEFAULT,DEFAULT);

--error 0,00942
drop table ori_tab2;
CREATE TABLE ori_tab2 (id int, c1 char (2000) DEFAULT 'ori_tab2',c2 varchar2(4000) DEFAULT '备注');
INSERT INTO ori_tab2 VALUES(1,DEFAULT,DEFAULT);

delimiter /;
-- echo # 存储过程
create or replace procedure ori_pro1(p1 in varchar default 'ori_pro1',p2 out number,p3 out varchar2,p4 out timestamp) is
begin
  p2 := 2024;
  p3 := p1;
  p4 := to_timestamp('04-06-2024 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff');
end;
/
-- echo # 函数
create or replace function ori_fun1(p1 number) return number is 
begin
  return p1*2;
end;
/
create or replace function ori_fun2(p1 varchar2) return varchar2 is 
begin
  return upper(p1);
end;
/
create or replace function ori_fun3(p1 timestamp) return timestamp is 
begin
  return next_day(p1,'monday');
end;
/
-- echo # 包
create or replace package ori_pck1 is
  a NUMBER;
  b varchar2(200) default 'ori_pck1';
  c timestamp default NULL;
  procedure pack_pro(p1 in out number);
  function pack_func(p1 in number) return number;
end;
/
create or replace package body ori_pck1 is
  procedure pack_pro(p1 in out number) is
  begin
    p1 := p1 *2;
  end;
  function pack_func(p1 in number) return number is
  begin
    a := p1;
    return p1;
  end;
end;
/
delimiter ;/

-- echo # 序列
--error 02289
drop sequence ori_seq1;
create sequence ori_seq1 start with 500 increment by 1 maxvalue 1000 cycle nocache;

-- echo # 同义词
create or replace synonym syn_ori_pro1 for ori_pro1;
create or replace synonym syn_ori_fun1 for ori_fun1;
create or replace synonym syn_ori_fun2 for ori_fun2;
create or replace synonym syn_ori_fun3 for ori_fun3;
create or replace synonym syn_ori_pck1 for ori_pck1;
create or replace synonym syn_ori_seq1 for ori_seq1;

delimiter /;
declare
  v1 varchar2(200) default '远程OB端';
  v2 number;
  v3 varchar2(200);
  v4 timestamp;
begin
  ori_pro1(v1,v2,p3=>v3,p4=>v4);
  DBMS_OUTPUT.PUT_LINE('存储过程结果：' || v2 || ',' || v3 || ',' || v4);
  v2 := ori_fun1(1);
  v3 := ori_fun2('function');
  v4 := ori_fun3(to_timestamp('04-06-2024 12:01:33.3333','dd-mm-yyyy hh24:mi:ss.ff'));
  DBMS_OUTPUT.PUT_LINE('函数结果：' || v2 || ',' || v3 || ',' || v4);
  ori_pck1.pack_pro(v2);
  DBMS_OUTPUT.PUT_LINE('包结果：' || v2);
  v2 := ori_pck1.pack_func(v2);
  DBMS_OUTPUT.PUT_LINE('包结果：' || v2);
  v2 := ori_seq1.nextval;
  DBMS_OUTPUT.PUT_LINE('序列结果：' || v2);
end;
/
select println() from dual;/
--error 904
declare
  v1 varchar2(200) default '远程OB端';
  v2 number;
  v3 varchar2(200);
  v4 timestamp;
begin
  v2 := syn_ori_syn1.nextval;
  DBMS_OUTPUT.PUT_LINE('序列结果：' || v2);
end;
/
delimiter ;/
