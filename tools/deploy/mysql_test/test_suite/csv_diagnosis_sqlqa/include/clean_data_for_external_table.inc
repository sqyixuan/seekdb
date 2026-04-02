let $RM_CMD = ssh $HOST_IP 'rm -rf "$OBSERVER_DIR/$DATA_FOLDER_NAME"' ;

exec $RM_CMD;