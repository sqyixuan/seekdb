--disable_query_log
let $SCP_CMD = osscmd listallobject oss://$DATA_FOLDER_NAME/$FILE_PREFIX -H $OSS_HOST -i $OSS_ACCESS_ID -k $OSS_ACCESS_KEY | cut -f 4 -d ' ' | grep -E '^oss';
exec $SCP_CMD;
--enable_query_log
