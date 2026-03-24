--require mysql_test/test_suite/charset/r/have_utf8mb4.require

--disable_query_log

SHOW COLLATION LIKE 'utf8mb4_general_ci';

--enable_query_log
