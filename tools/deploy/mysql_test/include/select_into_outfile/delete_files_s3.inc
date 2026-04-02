--disable_result_log
let $SCP_CMD = s3cmd del --recursive s3://$DATA_FOLDER_NAME/ --access_key=$S3_AK --secret_key=$S3_SK --region=$S3_REGION --host=$S3_HOST --host-bucket=S3_BUCKETs.$S3_HOST;
exec $SCP_CMD;
--enable_result_log
