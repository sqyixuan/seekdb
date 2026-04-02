--source mysql_test/test_suite/mysql8_t_2/include/max_indexes.inc

if ($max_indexes != 64U) {
  --skip Test needs a server built with --with-max-indexes=64U
}
