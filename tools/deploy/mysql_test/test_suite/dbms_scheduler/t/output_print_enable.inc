#owner: linlin.xll
#owner group: sql1
#description: enable dbms_output and create function print
#tags: pl

--disable_query_log
--result_format 4

--disable_warnings

delimiter /;

CREATE OR REPLACE FUNCTION print RETURN VARCHAR2 IS
  r VARCHAR2(4000); t INT; r1 VARCHAR2(4000);
BEGIN
  DBMS_OUTPUT.GET_LINE(r, t);
  WHILE t != 1 LOOP
    DBMS_OUTPUT.GET_LINE(r1, t);
    r := r || ' ' || r1;
  END LOOP;
  RETURN r;
END;
/
CALL DBMS_OUTPUT.ENABLE();
/

CREATE OR REPLACE PROCEDURE clear_output IS
  r VARCHAR2(32767); t INT;
BEGIN
  LOOP
    DBMS_OUTPUT.GET_LINE(r, t);
    EXIT WHEN t = 1;
  END LOOP;
END;
/

CREATE OR REPLACE FUNCTION println RETURN VARCHAR2 IS
  r VARCHAR2(32767); t INT; r1 VARCHAR2(32767);
BEGIN
  DBMS_OUTPUT.GET_LINE(r, t);
  LOOP
    DBMS_OUTPUT.GET_LINE(r1, t);
    EXIT WHEN t = 1;
    r := r || chr(10) || '  ' || r1;
  END LOOP;
  RETURN r;
END;
/
CALL DBMS_OUTPUT.ENABLE();
/

CREATE OR REPLACE PROCEDURE drop_object(o_name VARCHAR2) IS
  o_type VARCHAR2(256);
BEGIN
  SELECT object_type INTO o_type FROM ALL_OBJECTS WHERE UPPER(OBJECT_NAME) = UPPER(o_name) AND UPPER(OWNER) = UPPER(SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND ROWNUM = 1;
  IF o_type = 'PACKAGE' THEN
    EXECUTE IMMEDIATE 'DROP PACKAGE ' || o_name;
  ELSIF o_type = 'PACKAGE BODY' THEN
    EXECUTE IMMEDIATE 'DROP PACKAGE ' || o_name;
  ELSIF o_type = 'TYPE' THEN
    EXECUTE IMMEDIATE 'DROP TYPE ' || o_name;
  ELSIF o_type = 'TABLE' THEN
    EXECUTE IMMEDIATE 'DROP TABLE ' || o_name;
  ELSIF o_type = 'VIEW' THEN
    EXECUTE IMMEDIATE 'DROP VIEW ' || o_name;
  ELSIF o_type = 'TRIGGER' THEN
    EXECUTE IMMEDIATE 'DROP TRIGGER ' || o_name;
  ELSIF o_type = 'PROCEDURE' THEN
    EXECUTE IMMEDIATE 'DROP PROCEDURE ' || o_name;
  ELSIF o_type = 'FUNCTION' THEN
    EXECUTE IMMEDIATE 'DROP FUNCTION ' || o_name;
  ELSIF o_type = 'INDEX' THEN
    EXECUTE IMMEDIATE 'DROP INDEX ' || o_name;
  ELSIF o_type = 'SYNONYM'  THEN
    EXECUTE IMMEDIATE 'DROP SYNONYM ' || o_name;
  ELSIF o_type = 'SEQUENCE' THEN
    EXECUTE IMMEDIATE 'DROP SEQUENCE ' || o_name;
  END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
END;
/

delimiter ;/

--enable_query_log

