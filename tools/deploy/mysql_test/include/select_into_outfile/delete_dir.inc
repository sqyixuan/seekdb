--disable_query_log
let $SCP_CMD = ssh $HOST_IP 'rm -rf "$OBSERVER_DIR/$DATA_FOLDER_NAME/"';
exec $SCP_CMD;
--enable_query_log
