rm log/libobcdc*

OBI=ob1

CASE='lob_dml,generated_column_basic,sp_trans,bit_column_dml,unique_idx_verify_data,idx_unique_idx_basic_xiuming,timezone,parallel_insert,insert_rows_sum_of_2M_size,insert_a_row_of_2M_size,insert,update,update_test,replace_with_different_data_type,update_set_default,update_rowkey_trx,replace_null,replace,delete,delete_range,delete_range_in,datatype_dml,datatype_java,jp_update_utf8,jp_insert_utf8,strings_charsets_update_delete,charset_collation_set,9001_insert_all_column_type_ruoqing,9002_insert_single_multi_pk_ruoqing,9003_insert_multiple_rows_in_one_sql,9004_null_in_pk,9005_null_in_pk_delete,9006_update_replace_all_col_types,9007_delete_single_multiple_rows,9009_escape_char,hidden_pk'
CASE='lob_dml'

CASE="9001_insert_all_column_type_ruoqing,bit_column_dml,generated_column_basic,lob_type,unique_idx_verify_data,9002_insert_single_multi_pk_ruoqing,charset_collation_set,hidden_pk,parallel_insert,update_rowkey_trx,9003_insert_multiple_rows_in_one_sql,datatype_dml,idx_unique_idx_basic_xiuming,replace_null,9004_null_in_pk,datatype_java,insert_a_row_of_2M_size,replace,9005_null_in_pk_delete,ddl_trans,insert,replace_with_different_data_type,update_test,update,9006_update_replace_all_col_types,delete,delete_range_in,jp_insert_utf8,sp_trans,9007_delete_single_multiple_rows,delete_range,jp_update_utf8,strings_charsets_update_delete,9009_escape_char,delete,lob_dml,timezone,alter_partition_by"
#CASE='9003_insert_multiple_rows_in_one_sql'
# Path: mysql_test/test_suite/direct_load_data/t/direct_load_data.test
# CASE="direct_load_data.direct_load_data"

#### run observer cases ####
#./deploy.py ${OBI}.mysqltest testset="${CASE}" reboot
#./deploy.py ${OBI}.mysqltest testset="${CASE}" disable-reboot

#### run observer & obcdc cases ####
#./deploy.py ${OBI}.mysqltest testset="${CASE}" oblog_diff java
./deploy.py ${OBI}.mysqltest testset="${CASE}" disable-reboot oblog_diff java
#./deploy.py ${OBI}.mysqltest testset="${CASE}" oblog_diff java record
#./deploy.py ${OBI}.mysqltest testset="${CASE}" disable-reboot oblog_diff java record
