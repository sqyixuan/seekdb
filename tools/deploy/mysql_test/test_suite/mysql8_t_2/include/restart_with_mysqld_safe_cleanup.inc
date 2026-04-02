# ==== Purpose ====
#
# Restore normal configuration after use mysqld_safe.
#
# ==== Usage ====
#
# --source mysql_test/test_suite/mysql8_t_2/include/restart_with_mysqld_safe_cleanup.inc
#
# Please see include/restart_with_mysqld_safe.inc
#

--let $MYSQL_SOCKET= `SELECT @@socket`
--let $MYSQL_PORT= `SELECT @@port`
--let $_server_id= `SELECT @@server_id`

# Shutdown mysqld with mysqladmin
--exec $MYSQLADMIN -h localhost -S $MYSQL_SOCKET -P $MYSQL_PORT -u root shutdown 2>&1

# Delay introduced - mysqld_safe takes some time to restart mysqld
--source mysql_test/test_suite/mysql8_t_2/include/wait_until_disconnected.inc

# Restart mysqld of MTR
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.$_server_id.expect
--enable_reconnect
--source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc

--let $rpl_server_number= $_server_id
--source mysql_test/test_suite/mysql8_t_2/include/rpl_reconnect.inc
