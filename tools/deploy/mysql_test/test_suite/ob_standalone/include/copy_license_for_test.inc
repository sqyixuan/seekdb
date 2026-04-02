connect (obsys_for_copy,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_for_copy;

--disable_query_log
let $HOST_IP = query_get_value('select host_ip()',host_ip(), 1);
let $LICENSE_FOLDER_NAME = test_license;
let $SCP_CMD = ssh $HOST_IP 'mkdir -p "$OBSERVER_DIR/$LICENSE_FOLDER_NAME"' && scp -r $PWD/mysql_test/test_suite/ob_standalone/data/* $HOST_IP:"$OBSERVER_DIR/$LICENSE_FOLDER_NAME";
exec $SCP_CMD;
--enable_query_log

disconnect obsys_for_copy;

