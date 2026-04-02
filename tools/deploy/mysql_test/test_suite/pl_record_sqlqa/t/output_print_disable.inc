--disable_query_log
--result_format 4

--disable_warnings

DROP FUNCTION print;
DROP FUNCTION println;
CALL DBMS_OUTPUT.DISABLE();

--enable_query_log