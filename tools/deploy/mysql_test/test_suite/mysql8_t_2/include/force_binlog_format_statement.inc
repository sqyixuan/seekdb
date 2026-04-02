--source mysql_test/test_suite/mysql8_t_2/include/have_log_bin.inc

--disable_query_log
SET @previous_binlog_format__htnt542nh=@@GLOBAL.binlog_format;
--error 1238
SET @@GLOBAL.binlog_format=STATEMENT;
--error 1238
SET binlog_format=STATEMENT;
--enable_query_log
