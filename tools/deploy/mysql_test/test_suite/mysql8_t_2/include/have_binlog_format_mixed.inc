--source mysql_test/test_suite/mysql8_t_2/include/have_log_bin.inc

let $have_binlog_format_mixed= `SELECT @@binlog_format LIKE 'MIXED'`;
if (!$have_binlog_format_mixed)
{
  --skip Test requires binlog_format = MIXED
}
