--disable_query_log
let $SCP_CMD = osscmd mkdir oss://$DATA_FOLDER_NAME -H $OSS_HOST -i $OSS_ACCESS_ID -k $OSS_ACCESS_KEY;
exec $SCP_CMD;
--enable_query_log
