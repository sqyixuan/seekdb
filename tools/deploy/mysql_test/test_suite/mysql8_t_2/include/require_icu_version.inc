source mysql_test/test_suite/mysql8_t_2/include/icu_utils.inc;
let $icu_major_version=`SELECT icu_major_version()`;
source mysql_test/test_suite/mysql8_t_2/include/cleanup_icu_utils.inc;

if ($icu_major_version < $required_icu_version)
{
  skip Not run for ICU versions below $required_icu_version;
}
