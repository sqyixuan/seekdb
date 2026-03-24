let $MSG = CAT_FILE: "$DATA_FOLDER_NAME/$FILE_NAME";
--echo
--echo $MSG
let $SCP_CMD = ssh $HOST_IP 'cat "$OBSERVER_DIR/$DATA_FOLDER_NAME/$FILE_NAME"';
--disable_query_log
exec $SCP_CMD;
--enable_query_log
--echo 

