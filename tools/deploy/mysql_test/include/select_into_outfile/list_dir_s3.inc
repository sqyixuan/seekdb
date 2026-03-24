--disable_query_log
let $SCP_CMD = s3cmd ls -r s3://$DATA_FOLDER_NAME --access_key=$S3_AK --secret_key=$S3_SK --region=$S3_REGION --host=$S3_HOST --host-bucket=S3_BUCKETs.$S3_HOST | rev | cut -d' ' -f1 | rev | sed 's/outfile_s3_[0-9]*/outfile_s3/g';
exec $SCP_CMD;
--enable_query_log
