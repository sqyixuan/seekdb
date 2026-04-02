--disable_query_log
let $SCP_CMD = osscmd listallobject oss://antsys-antsys-oceanbase-rd-sql/jim.wjh/mysqltest_regression_data/zhch_external_table_data/outfile_partition/__NULL__ -H $OSS_HOST -i $OSS_ACCESS_ID -k $OSS_ACCESS_KEY | cut -f 3,4 -d ' ';
exec $SCP_CMD;

let $SCP_CMD = osscmd listallobject oss://antsys-antsys-oceanbase-rd-sql/jim.wjh/mysqltest_regression_data/zhch_external_table_data/outfile_partition/par_-1 -H $OSS_HOST -i $OSS_ACCESS_ID -k $OSS_ACCESS_KEY | cut -f 3,4 -d ' ';
exec $SCP_CMD;

let $SCP_CMD = osscmd listallobject oss://antsys-antsys-oceanbase-rd-sql/jim.wjh/mysqltest_regression_data/zhch_external_table_data/outfile_partition/par_1 -H $OSS_HOST -i $OSS_ACCESS_ID -k $OSS_ACCESS_KEY | cut -f 3,4 -d ' ';
exec $SCP_CMD;

--enable_query_log
