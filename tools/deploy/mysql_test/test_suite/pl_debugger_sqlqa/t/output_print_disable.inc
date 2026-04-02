--disable_query_log
--result_format 4

--disable_warnings

DROP FUNCTION print;
DROP FUNCTION println;
DROP TABLE initialize_table;
CALL DBMS_OUTPUT.DISABLE();

--enable_warnings
--enable_query_log
