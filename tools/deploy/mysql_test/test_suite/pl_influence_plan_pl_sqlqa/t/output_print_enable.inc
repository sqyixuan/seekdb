--disable_query_log
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
CREATE OR REPLACE FUNCTION println RETURN CLOB  IS
  r CLOB ; t INT; r1 CLOB ;
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
delimiter ;/
--enable_warnings
--enable_query_log
