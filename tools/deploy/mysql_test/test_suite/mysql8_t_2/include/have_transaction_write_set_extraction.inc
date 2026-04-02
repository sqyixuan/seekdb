# ==== Purpose ====
#
# Ensure that the server has transaction_write_set_extraction enabled.
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/have_transaction_write_set_extraction.inc

if (`SELECT @@GLOBAL.transaction_write_set_extraction = 'OFF'`)
{
  --skip Test requires transaction_write_set_extraction enabled
}
