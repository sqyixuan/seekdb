--echo ------------------ hu_check_exit_and_restart.inc ------------------

sleep 3;
ob1.proxy0.check_stop;

ob1.proxy0.nstart;
sleep 3;
ob1.proxy0.exist;

$cmd_header update test.ob_all_proxy set new_binary_version = '$new_binary_version' where proxy_ip = "$ip" and proxy_port = $port;
$cmd_header update test.ob_all_proxy set new_binary_6u_md5 = '$new_binary_6u_md5' where proxy_ip = "$ip" and proxy_port = $port;

--echo ------------------ hu_check_exit_and_restart.inc succ ------------------
