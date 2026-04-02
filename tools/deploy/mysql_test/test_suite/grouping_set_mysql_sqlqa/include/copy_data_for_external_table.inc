connect (obsys_for_external_table,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_for_external_table;


let $HOST_IP = query_get_value('select host_ip()',host_ip(), 1);

let $DATA_FOLDER_NAME = mysqltest_data_for_external_table;

let $SCP_CMD = ssh $HOST_IP 'mkdir -p "$OBSERVER_DIR/$DATA_FOLDER_NAME"' && scp -r $PWD/mysql_test/test_suite/external_table_sqlqa/data/test.csv $HOST_IP:"$OBSERVER_DIR/$DATA_FOLDER_NAME";

exec $SCP_CMD;


disconnect obsys_for_external_table;
connection default;