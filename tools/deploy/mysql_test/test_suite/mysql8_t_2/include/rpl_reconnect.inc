# ==== Purpose ====
#
# After a server has restarted, this waits for all clients configured
# by rpl_init.inc and/or master-slave.inc to reconnect again.
#
# For each connection, it issues this:
#   --enable_reconnect
#   --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
#   --disable_reconnect
#
#
# ==== Usage ====
#
# --let $rpl_server_number= N
# [--let $rpl_extra_connections_per_server= 1]
# [--let $rpl_debug= 1]
# --source mysql_test/test_suite/mysql8_t_2/include/rpl_reconnect.inc
#
# Parameters:
#   $rpl_server_number
#     Number to identify the server that needs to reconnect.  1 is the
#     master server, 2 the slave server, 3 the 3rd server, and so on.
#     Cf. include/rpl_init.inc
#
#   $rpl_extra_connections_per_server, $rpl_debug
#     See include/rpl_init.inc

--let $include_filename= rpl_reconnect.inc
--source mysql_test/test_suite/mysql8_t_2/include/begin_include_file.inc

if (!$rpl_server_number)
{
  --die ERROR IN TEST: you must set $rpl_server_number before you source rpl_connect.inc
}

if ($_rpl_extra_connections_per_server == '')
{
  --let $_rpl_extra_connections_per_server= 1
}


if ($rpl_debug)
{
  --echo ---- Enable reconnect ----
}

if ($rpl_server_number == 1)
{
  --let $rpl_connection_name= default
  --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
  --enable_reconnect

  if ($_have_default_connections)
  {
    --let $rpl_connection_name= master
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --enable_reconnect

    --let $rpl_connection_name= master1
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --enable_reconnect

    if ($rpl_group_replication)
    {
      --let $rpl_connection_name= server1
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --enable_reconnect

      --let $rpl_connection_name= server_1
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --enable_reconnect
    }
  }
}

if ($rpl_server_number == 2)
{
  if ($_have_default_connections)
  {
    --let $rpl_connection_name= slave
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --enable_reconnect

    --let $rpl_connection_name= slave1
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --enable_reconnect

    if ($rpl_group_replication)
    {
      --let $rpl_connection_name= server2
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --enable_reconnect

      --let $rpl_connection_name= server_2
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --enable_reconnect
    }
  }
}

if ($using_rpl_group_replication_default_connections)
{
  --let $rpl_connection_name= server$rpl_server_number
  --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
  --enable_reconnect
}

--let $rpl_connection_name= server_$rpl_server_number
--source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
--enable_reconnect

--let $_rpl_connection_number= 1
while ($_rpl_connection_number <= $_rpl_extra_connections_per_server)
{
  --let $rpl_connection_name= server_$rpl_server_number$underscore$_rpl_connection_number
  --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
  --enable_reconnect
  --inc $_rpl_connection_number
}

if ($rpl_debug)
{
  --echo ---- Wait for reconnect and disable reconnect on all connections ----
}

--let $_rpl_server_number= $rpl_server_number

if ($rpl_server_number == 1)
{
  --let $rpl_connection_name= default
  --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
  --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
  --disable_reconnect

  if ($_have_default_connections)
  {
    --let $rpl_connection_name= master
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
    --disable_reconnect

    --let $rpl_connection_name= master1
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
    --disable_reconnect

    if ($rpl_group_replication)
    {
      --let $rpl_connection_name= server1
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
      --disable_reconnect

      --let $rpl_connection_name= server_1
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
      --disable_reconnect
    }
  }
}

if ($rpl_server_number == 2)
{
  if ($_have_default_connections)
  {
    --let $rpl_connection_name= slave
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
    --disable_reconnect

    --let $rpl_connection_name= slave1
    --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
    --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
    --disable_reconnect

    if ($rpl_group_replication)
    {
      --let $rpl_connection_name= server2
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
      --disable_reconnect

      --let $rpl_connection_name= server_2
      --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
      --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
      --disable_reconnect
    }
  }
}

if ($using_rpl_group_replication_default_connections)
{
  --let $rpl_connection_name= server$rpl_server_number
  --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
  --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
  --disable_reconnect
}

--let $rpl_connection_name= server_$rpl_server_number
--source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
--source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
--disable_reconnect

--let $_rpl_connection_number= 1
while ($_rpl_connection_number <= $_rpl_extra_connections_per_server)
{
  --let $rpl_connection_name= server_$rpl_server_number$underscore$_rpl_connection_number
  --source mysql_test/test_suite/mysql8_t_2/include/rpl_connection.inc
  --source mysql_test/test_suite/mysql8_t_2/include/wait_until_connected_again.inc
  --disable_reconnect
  --inc $_rpl_connection_number
}


--let $include_filename= rpl_reconnect.inc
--source mysql_test/test_suite/mysql8_t_2/include/end_include_file.inc
