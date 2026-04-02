--source mysql_test/test_suite/mysql8_t_2/include/resource_group_init.inc

if ($thr_prio_enable == 0)
{
  skip Skipping test. (Test requires thread priority to be supported.);
}

