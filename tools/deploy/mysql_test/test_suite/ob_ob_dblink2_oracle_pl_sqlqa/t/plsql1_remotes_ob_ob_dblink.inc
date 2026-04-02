#owner: webber.wb
#owner group: 技术部-SQL-存储过程
#description: OB-oracle到OB-oracle PL dblink 复杂数据类型测试
#tags: pl
#author:whh01047778

--source mysql_test/test_suite/ob_ob_dblink2_oracle_pl_sqlqa/include/output_print_enable.inc

--echo #-----------------------------------------------------------------------------
--echo # 远程OB端 dblink_tenant1租户下
--echo #-----------------------------------------------------------------------------

--error 0,00942
drop table ori_tab1;
CREATE TABLE ori_tab1 (id int, c1 char(2000) DEFAULT 'ori_tab1',c2 varchar2(4000) DEFAULT '远程OB端');
INSERT INTO ori_tab1 VALUES(1,DEFAULT,DEFAULT);

--echo #### case1
--error 0,00942
drop table a_18;
create table a_18(a1 number);
insert into a_18 values(-1);

delimiter /;
create or replace function f1_19 return integer is 
begin
  return 1000;
end;
/
create or replace function f2_19(i int) return int is
begin
  return i;
end;
/
create or replace function f3_19(j SMALLINT) return int is
begin
  return j;
end;
/

create or replace procedure p(x1 number) is
  TYPE tab_type IS TABLE OF number;
  abs tab_type;
  cursor c is select a1 from a_18;
  x number := x1;
begin
  open c;
  fetch c bulk collect into abs limit 1000;
  x := abs(1);
  insert into a_18 values(x);
  close c;
end;
/

--echo #### case2  
#嵌套表中为record类型
CREATE OR REPLACE PACKAGE pkg_23 IS
  TYPE record_type IS RECORD (
    field1 NUMBER,
    field2 DATE,
    field3 VARCHAR2(100));
  
  TYPE table_type IS TABLE OF record_type;

  procedure p0;
  FUNCTION func_23 RETURN table_type;
END pkg_23;
/

CREATE OR REPLACE PACKAGE BODY pkg_23 IS
  procedure p0 is
  begin
    null;
  end;
  FUNCTION func_23 RETURN table_type IS
    -- 嵌套表变量
    table_variable table_type;
    -- record变量
    record_variable record_type;
  BEGIN
    table_variable := table_type();
    
    -- record赋值
    record_variable.field1 := 123;
    record_variable.field2 := TO_DATE('2022-01-01', 'YYYY-MM-DD');
    record_variable.field3 := 'Hello';
    
    --嵌套表赋值
    table_variable.EXTEND;
    table_variable(1) := record_variable;
    
    RETURN table_variable;
  END func_23;
END pkg_23;
/
call pkg_23.p0();
/

--echo #### case3
#嵌套表中为字符类型
CREATE OR REPLACE PACKAGE pkg_24 IS
  TYPE nested_table_type IS TABLE OF VARCHAR2(100);
  
  procedure p0;
  FUNCTION func_24 RETURN nested_table_type;
END pkg_24;
/
CREATE OR REPLACE PACKAGE BODY pkg_24 IS
  procedure p0 is
  begin
    null;
  end;
  FUNCTION func_24 RETURN nested_table_type IS
    nested_table nested_table_type := nested_table_type('hello world');
  BEGIN
    RETURN nested_table; 
  END func_24;
END pkg_24;
/
call pkg_24.p0();
/

--echo #### case4
CREATE OR REPLACE PACKAGE pkg_25 IS
  TYPE nested_table_type IS TABLE OF NUMBER;
  
  procedure p0;
  PROCEDURE proc_25(p_values IN nested_table_type);
END pkg_25;
/

CREATE OR REPLACE PACKAGE BODY pkg_25 IS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE proc_25(p_values IN nested_table_type) IS
  BEGIN
    FOR i IN 1..p_values.COUNT LOOP
      DBMS_OUTPUT.PUT_LINE(p_values(i));
    END LOOP;
  END proc_25;
END pkg_25;
/
call pkg_25.p0();
/

--echo #### case5
CREATE OR REPLACE PACKAGE pkg_29 IS
  TYPE nested_table_type IS TABLE OF VARCHAR2(100);
  
  procedure p0;
  FUNCTION func_29(p_input IN VARCHAR2) RETURN nested_table_type;
END pkg_29;
/

CREATE OR REPLACE PACKAGE BODY pkg_29 IS
  procedure p0 is
  begin
    null;
  end;
  FUNCTION func_29(p_input IN VARCHAR2) RETURN nested_table_type IS
    nested_table nested_table_type := nested_table_type(p_input);
  BEGIN
    RETURN nested_table; 
  END func_29;
END pkg_29;
/
call pkg_29.p0();
/

--echo #### case6
#my_package 
CREATE OR REPLACE PACKAGE my_package AS
  TYPE my_type IS RECORD (
    id NUMBER,
    name VARCHAR2(50),
    age NUMBER);
  
  procedure p0;
  PROCEDURE my_procedure(p_in_data IN my_type, p_out_message OUT VARCHAR2);
END my_package;
/

CREATE OR REPLACE PACKAGE BODY my_package AS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE my_procedure(p_in_data IN my_type, p_out_message OUT VARCHAR2) IS
  BEGIN
    p_out_message := 'Hello, ' || p_in_data.name || '! Your age is ' || p_in_data.age || '.';
  END my_procedure;
END my_package;
/
call my_package.p0();
/

--echo #### case7
#MY_PACKAGE_2
CREATE OR REPLACE PACKAGE MY_PACKAGE_2 AS
  TYPE MY_RECORD IS RECORD (
    ID NUMBER,
    NAME VARCHAR2(100),
    AGE NUMBER);
  
  procedure p0;
  PROCEDURE MY_PROCEDURE(p_in_data IN MY_RECORD, p_out_data OUT MY_RECORD);
END MY_PACKAGE_2;
/

CREATE OR REPLACE PACKAGE BODY MY_PACKAGE_2 AS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE MY_PROCEDURE(p_in_data IN MY_RECORD, p_out_data OUT MY_RECORD) IS
  BEGIN
    p_out_data := p_in_data;
    p_out_data.NAME := UPPER(p_in_data.NAME);
  END MY_PROCEDURE;
END MY_PACKAGE_2;
/
delimiter ;/
call MY_PACKAGE_2.p0();

--echo #### case8
#my_package_3
--error 0,00942
drop table employees;
CREATE TABLE employees (id NUMBER,name VARCHAR2(100),age NUMBER,department_id NUMBER);
INSERT INTO employees (id, name, age, department_id) VALUES (1, 'John', 30, 1);
INSERT INTO employees (id, name, age, department_id) VALUES (2, 'Jane', 25, 1);
INSERT INTO employees (id, name, age, department_id) VALUES (3, 'Bob', 40, 2);
INSERT INTO employees (id, name, age, department_id) VALUES (4, 'Alice', 35, 2);

delimiter /;
CREATE OR REPLACE PACKAGE my_package_3 AS
  type my_record is record(  
  id NUMBER,
  name VARCHAR2(100),
  age NUMBER,
  department_id NUMBER);

  TYPE emp_list_type IS TABLE OF my_record;

  procedure p0;
 --out出参使用table of 
  PROCEDURE get_employee_list(
    dept_id IN NUMBER,
    emp_list OUT emp_list_type);
END my_package_3;
/

CREATE OR REPLACE PACKAGE BODY my_package_3 AS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE get_employee_list(
    dept_id IN NUMBER,
    emp_list OUT emp_list_type
  ) IS
  BEGIN
    SELECT * BULK COLLECT INTO emp_list
    FROM employees
    WHERE department_id = dept_id;
  END;
END my_package_3;
/
call my_package_3.p0();
/

--echo #### case9
#my_package_4
CREATE OR REPLACE PACKAGE my_package_4 AS
  TYPE emp_type IS RECORD (
    id NUMBER,
    name VARCHAR2(100),
    age NUMBER,
    department_id NUMBER);

  TYPE emp_list_type IS TABLE OF emp_type INDEX BY PLS_INTEGER;

  procedure p0;
  PROCEDURE process_employee_list(
    emp_list IN OUT emp_list_type);

  PROCEDURE process_employee_age(
    emp_list IN OUT emp_list_type);

END my_package_4;
/

CREATE OR REPLACE PACKAGE BODY my_package_4 AS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE process_employee_list(
    emp_list IN OUT emp_list_type
  ) IS
    BEGIN
      FOR i IN 1..emp_list.COUNT LOOP
        DBMS_OUTPUT.PUT_LINE('ID: ' || emp_list(i).id || ', Name: ' || emp_list(i).name || ', Age: ' || emp_list(i).age || ', Department ID: ' || emp_list(i).department_id);
      END LOOP;
    END;

  PROCEDURE process_employee_age(
    emp_list IN OUT emp_list_type
  ) IS
    -- 遍历输入的Nested Table，将其中员工的年龄自增5
    BEGIN
      FOR i IN 1..emp_list.COUNT LOOP
        emp_list(i).age := emp_list(i).age + 5;
      END LOOP;
    END;
END my_package_4;
/
delimiter ;/
call my_package_4.p0();

--echo #### case10
delimiter /;
CREATE OR REPLACE PACKAGE test_package001 AS
  -- 定义 RECORD 类型
  TYPE employee_record IS RECORD (
    emp_id NUMBER,
    emp_name VARCHAR2(100),
    emp_salary NUMBER
  );
  
  procedure p0;
  -- 存储过程，接收 IN 参数为 RECORD 类型，OUT 参数为 RECORD 类型
  PROCEDURE update_employee(
    emp_in IN employee_record,
    emp_out OUT employee_record
  );
END test_package001;
/
CREATE OR REPLACE PACKAGE BODY test_package001 AS
  procedure p0 is
  begin
    null;
  end;
  -- 存储过程实现，将 IN 参数的 emp_salary 加倍，并返回 OUT 参数
  PROCEDURE update_employee(
    emp_in IN employee_record,
    emp_out OUT employee_record
  ) IS
  BEGIN
    emp_out.emp_id := emp_in.emp_id;
    emp_out.emp_name := emp_in.emp_name;
    emp_out.emp_salary := emp_in.emp_salary * 2;
  END;

END test_package001;
/
call test_package001.p0();
/

--echo #### case11
CREATE OR REPLACE PACKAGE test_package002 AS
  -- 定义 RECORD 类型
  TYPE employee_record IS RECORD (
    emp_id NUMBER,
    emp_name VARCHAR2(100),
    emp_salary NUMBER
  );

  procedure p0;
  -- 存储过程，接收 IN 参数为 RECORD 类型，OUT 参数为 RECORD 类型
  PROCEDURE update_employee(
    emp_out OUT employee_record
  );

END test_package002;
/

CREATE OR REPLACE PACKAGE BODY test_package002 AS
  procedure p0 is
  begin
    null;
  end;
  -- 存储过程实现，将 IN 参数的 emp_salary 加倍，并返回 OUT 参数
  PROCEDURE update_employee(
    emp_out OUT employee_record
  ) IS
  BEGIN
    emp_out.emp_id := 1;
    emp_out.emp_name := 'John';
    emp_out.emp_salary := 2000;
  END;

END test_package002;
/
delimiter ;/
call test_package002.p0();

--echo #### case12
--disable_warnings
--error 0,942
drop table A0K_TEST_0629 CASCADE CONSTRAINTS purge;
--enable_warnings

create table A0K_TEST_0629(id number,name varchar2(20));
INSERT INTO A0K_TEST_0629 VALUES (1,'aaa');
COMMIT;

#在远程数据库上创建包
delimiter /;
CREATE OR REPLACE PACKAGE test_package003 AS
  -- 定义嵌套表类型
  TYPE employee_list IS TABLE OF VARCHAR2(100);

  procedure p0;
  -- 定义存储过程，参数为嵌套表类型
  PROCEDURE update_employee_list(p_list OUT employee_list);
END;
/

CREATE OR REPLACE PACKAGE BODY test_package003 AS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE update_employee_list(p_list OUT employee_list) AS
  BEGIN
    -- 查询员工列表并将结果存储到嵌套表中
    SELECT name BULK COLLECT INTO p_list FROM A0K_TEST_0629;
  END;
END;
/
call test_package003.p0();
/

--echo #### case13
#在远程数据库上创建包
CREATE OR REPLACE PACKAGE test_package004 AS
  -- 定义 VARRAY 类型
  TYPE employee_varray IS VARRAY(10) OF VARCHAR2(100);

  procedure p0;
  -- 定义存储过程，参数为 IN OUT 类型
  PROCEDURE update_employee_varray(p_varray IN OUT employee_varray, p_count OUT NUMBER);
END;
/

CREATE OR REPLACE PACKAGE BODY test_package004 AS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE update_employee_varray(p_varray IN OUT employee_varray, p_count OUT NUMBER) AS
  BEGIN
    -- 在此处编写存储过程的实现代码
    IF p_varray is null THEN 
      DBMS_OUTPUT.PUT_LINE('Empty'); 
    ELSE 
      FOR i IN 1 .. p_count LOOP
        p_varray(i) := p_varray(i) || ' (updated)';
      END LOOP;
    end if;
  END;
END;
/
delimiter ;/
call test_package004.p0();

--echo #### case14
--disable_warnings
--error 0,942
drop table A0K_TEST_0630 CASCADE CONSTRAINTS purge;
--enable_warnings

CREATE TABLE A0K_TEST_0630(test_id NUMBER NOT NULL);
INSERT INTO A0K_TEST_0630 VALUES(101);
INSERT INTO A0K_TEST_0630 VALUES(102);
INSERT INTO A0K_TEST_0630 VALUES(103);
INSERT INTO A0K_TEST_0630 VALUES(103);
INSERT INTO A0K_TEST_0630 VALUES(102);
COMMIT;

#在远程数据库上创建包
delimiter /;
CREATE OR REPLACE PACKAGE test_package005 AS
  -- 定义 VARRAY 类型
  TYPE num_table IS TABLE OF A0K_TEST_0630.test_id%TYPE
                  INDEX BY BINARY_INTEGER;  -- 定义关联数组的索引类型是整型

  procedure p0;
  -- 定义存储过程，参数为 IN OUT 类型
  PROCEDURE update_employee_varray(num_list OUT num_table);
END;
/

CREATE OR REPLACE PACKAGE BODY test_package005 AS
  procedure p0 is
  begin
    null;
  end;
  PROCEDURE update_employee_varray(num_list OUT num_table) AS
  BEGIN
     SELECT test_id BULK COLLECT INTO num_list 
     from A0K_TEST_0630;
     DBMS_OUTPUT.PUT_LINE('Bulk Collected:');

    FOR i IN num_list.FIRST..num_list.LAST LOOP
     IF i <= 3 THEN
        DBMS_OUTPUT.PUT_LINE('Number ['||num_list(i)||']');
     END IF;
   END LOOP;
  END;
END;
/
delimiter ;/
call test_package005.p0();

--source mysql_test/test_suite/ob_ob_dblink2_oracle_pl_sqlqa/include/output_print_disable.inc
