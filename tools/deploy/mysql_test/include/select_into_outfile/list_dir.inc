--disable_query_log
let $SCP_CMD = ssh $HOST_IP 'cd $OBSERVER_DIR/$DATA_FOLDER_NAME && ls';
exec $SCP_CMD;
--enable_query_log
