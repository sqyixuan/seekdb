#owner: linlin.xll
#owner group: sql1
#description: disable dbms_output and create function print
#tags: pl/sql

--disable_query_log
--result_format 4

--disable_warnings

DROP FUNCTION print;
DROP FUNCTION println;
DROP PROCEDURE drop_object;
CALL DBMS_OUTPUT.DISABLE();

--enable_query_log

