#odps
export ODPS_ACCESSID=`vault kv get 'secret/data/mysqltest/dev/odps_access_info' | grep access_id | awk '{print \$2}'`
export ODPS_ENDPOINT=`vault kv get 'secret/data/mysqltest/dev/odps_access_info' | grep end_point | awk '{print \$2}'`
export ODPS_ACCESSKEY=`vault kv get 'secret/data/mysqltest/dev/odps_access_info' | grep access_key | awk '{print \$2}'`
#oss
export OSS_AK=`vault kv get 'secret/data/mysqltest/dev/oss_access_info' | grep access_id | awk '{print \$2}'`
export OSS_SK=`vault kv get 'secret/data/mysqltest/dev/oss_access_info' | grep access_key | awk '{print \$2}'`
export OSS_HOST=`vault kv get 'secret/data/mysqltest/dev/oss_access_info' | grep host | awk '{print \$2}'`
export OSS_BUCKET=`vault kv get 'secret/data/mysqltest/dev/oss_access_info' | grep bucket | awk '{print \$2}'`
export OSS_ACCESS_ID=`vault kv get 'secret/data/mysqltest/dev/oss_access_info' | grep access_id | awk '{print \$2}'`
export OSS_ACCESS_KEY=`vault kv get 'secret/data/mysqltest/dev/oss_access_info' | grep access_key | awk '{print \$2}'`
#cos
export COS_AK=`vault kv get 'secret/data/mysqltest/dev/cos_access_info' | grep access_key | awk '{print \$2}'`
export COS_SK=`vault kv get 'secret/data/mysqltest/dev/cos_access_info' | grep secret_key | awk '{print \$2}'`
export COS_HOST=`vault kv get 'secret/data/mysqltest/dev/cos_access_info' | grep host | awk '{print \$2}'`
export COS_BUCKET=`vault kv get 'secret/data/mysqltest/dev/cos_access_info' | grep bucket | awk '{print \$2}'`
export COS_REGION=`vault kv get 'secret/data/mysqltest/dev/cos_access_info' | grep region | awk '{print \$2}'`
export COS_APPID=`vault kv get 'secret/data/mysqltest/dev/cos_access_info' | grep appid | awk '{print \$2}'`
#s3
export S3_AK=`vault kv get 'secret/data/mysqltest/dev/s3_access_info' | grep access_key | awk '{print \$2}'`
export S3_SK=`vault kv get 'secret/data/mysqltest/dev/s3_access_info' | grep secret_key | awk '{print \$2}'`
export S3_HOST=`vault kv get 'secret/data/mysqltest/dev/s3_access_info' | grep host | awk '{print \$2}'`
export S3_BUCKET=`vault kv get 'secret/data/mysqltest/dev/s3_access_info' | grep bucket | awk '{print \$2}'`
export S3_REGION=`vault kv get 'secret/data/mysqltest/dev/s3_access_info' | grep region | awk '{print \$2}'`

# AI FUNCTION
export OB_API_HOST=`vault kv get 'secret/data/mysqltest/dev/ai_embed_config' | grep api_host | awk '{print \$2}'`
export OB_API_KEY=`vault kv get 'secret/data/mysqltest/dev/ai_embed_config' | grep api_key | awk '{print \$2}'`
export OB_MODEL_NAME=`vault kv get 'secret/data/mysqltest/dev/ai_embed_config' | grep model_name | awk '{print \$2}'`
export OB_MODEL_PROVIDER=`vault kv get 'secret/data/mysqltest/dev/ai_embed_config' | grep provider | awk '{print \$2}'`