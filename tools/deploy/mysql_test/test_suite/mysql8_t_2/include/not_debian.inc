--source mysql_test/test_suite/mysql8_t_2/include/linux_distro.inc

if ($linux_distro == "debian") {
  --skip Test not supported on Debian
}
