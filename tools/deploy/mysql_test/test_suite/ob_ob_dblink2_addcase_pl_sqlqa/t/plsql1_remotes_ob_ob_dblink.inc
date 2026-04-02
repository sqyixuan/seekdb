#owner: webber.wb
#owner group: 技术部-SQL-存储过程
#description: OB-oracle到OB-oracle PL dblink 复杂数据类型测试
#tags: pl
#author:whh01047778

--source mysql_test/test_suite/ob_ob_dblink2_addcase_pl_sqlqa/include/output_print_enable.inc

--echo #-----------------------------------------------------------------------------
--echo # 远程OB端 dblink_tenant1租户下
--echo #-----------------------------------------------------------------------------

--error 0,00942
drop table ori_tab1;
CREATE TABLE ori_tab1 (id int, c1 char(2000) DEFAULT 'ori_tab1',c2 varchar2(4000) DEFAULT '远程OB端');
INSERT INTO ori_tab1 VALUES(1,DEFAULT,DEFAULT);

--echo #### case1  
#record类型
delimiter /;
create or replace package pkg00 as 
  type rd is record(id NUMBER, name VARCHAR2(100)); 
  procedure p1(a rd);
end;
/
create or replace package body pkg00 as  procedure p1(a rd) is begin null;end;end;
/

--echo #### case2  
#嵌套表中为record类型
CREATE OR REPLACE PACKAGE pkg01 IS
  TYPE record_type IS RECORD (
    field1 NUMBER,
    field2 DATE,
    field3 VARCHAR2(100));
  
  TYPE table_type IS TABLE OF record_type;
  FUNCTION func_23 RETURN table_type;
END pkg01;
/

CREATE OR REPLACE PACKAGE BODY pkg01 IS
  FUNCTION func_23 RETURN table_type IS
    table_variable table_type;
    record_variable record_type;
  BEGIN
    table_variable := table_type();
    
    -- record赋值
    select 123,TO_DATE('2022-01-01', 'YYYY-MM-DD'),'Hello' into record_variable from dual;

    --嵌套表赋值
    table_variable.EXTEND;
    table_variable(1) := record_variable;
    
    RETURN table_variable;
  END func_23;
END pkg01;
/
delimiter ;/

--echo #### case3 嵌套表中为字符类型
--error 0,00942
drop table ori_tab2;
CREATE TABLE ori_tab2 (id int, c1 VARCHAR2(2000) DEFAULT 'ori_tab0');
INSERT INTO ori_tab2 VALUES(1,DEFAULT);
INSERT INTO ori_tab2 VALUES(2,'DEFAULT');
INSERT INTO ori_tab2 VALUES(3,'ori_tab2');
INSERT INTO ori_tab2 VALUES(4,'pkg02');
INSERT INTO ori_tab2 VALUES(5,'nested_table_type');

delimiter /;
CREATE OR REPLACE PACKAGE pkg02 IS
  TYPE nested_table_type IS TABLE OF VARCHAR2(2000);
  
  procedure prints (p1 nested_table_type);
END pkg02;
/
CREATE OR REPLACE PACKAGE BODY pkg02 IS
  procedure prints (p1 nested_table_type) IS
  BEGIN
    FOR i IN 1..p1.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE(p1(i)||', ');
    END LOOP;
  END prints;
END pkg02;
/  

--echo #### case4
CREATE OR REPLACE PACKAGE pkg03 IS
  TYPE nested_table_type IS TABLE OF NUMBER;
  
  PROCEDURE pro(p_values IN nested_table_type);
END pkg03;
/

CREATE OR REPLACE PACKAGE BODY pkg03 IS
  PROCEDURE pro(p_values IN nested_table_type) IS
  BEGIN
    FOR i IN 1..p_values.COUNT LOOP
      DBMS_OUTPUT.PUT_LINE(p_values(i));
    END LOOP;
  END pro;
END pkg03;
/
delimiter ;/

--echo #### case5 
--error 0,00942
drop table ori_tab5;
CREATE TABLE ori_tab5 (id int, name VARCHAR2(2000) DEFAULT 'ori_tab0');
INSERT INTO ori_tab5 VALUES(1,'a');
INSERT INTO ori_tab5 VALUES(2,'b');
INSERT INTO ori_tab5 VALUES(3,'c');
INSERT INTO ori_tab5 VALUES(4,'d');
INSERT INTO ori_tab5 VALUES(5,'e');

delimiter /;
create or replace package pkg04 as 
  type rd is record(id NUMBER, name VARCHAR2(100)); 
  procedure p1(a rd);
end;
/
create or replace package body pkg04 as  
  procedure p1(a rd) is 
  begin 
    null;
  end;
end;
/
delimiter ;/

--source mysql_test/test_suite/ob_ob_dblink2_addcase_pl_sqlqa/include/output_print_disable.inc
