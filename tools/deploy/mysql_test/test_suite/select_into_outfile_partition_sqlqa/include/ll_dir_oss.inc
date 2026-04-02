--disable_query_log
let $SCP_CMD = osscmd listallobject oss://$DATA_FOLDER_NAME/ -H $OSS_HOST -i $OSS_ACCESS_ID -k $OSS_ACCESS_KEY | cut -f 3,4 -d ' ' | sed  's/outfile_partition_[0-9]*/outfile_partition/g' ;
exec $SCP_CMD;
--enable_query_log
