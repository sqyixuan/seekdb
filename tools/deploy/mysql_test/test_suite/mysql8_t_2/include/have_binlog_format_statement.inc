--source mysql_test/test_suite/mysql8_t_2/include/have_log_bin.inc

let $have_binlog_format_statement= `SELECT @@binlog_format LIKE 'STATEMENT'`;
if (!$have_binlog_format_statement)
{
  --skip Test requires binlog_format = STATEMENT
}
