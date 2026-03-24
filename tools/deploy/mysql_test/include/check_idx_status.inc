##
--disable_query_log
--disable_result_log

let $__index_success_status__ = 2;

connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;

#let $row_id = query_get_value(select row_id from oceanbase.__all_core_table where table_name = '__all_table' and column_name = 'table_name' and column_value = '__all_table', row_id, 1);
#let $part_num = query_get_value(select column_value from oceanbase.__all_core_table where table_name = '__all_table' and row_id = $row_id and column_name = 'part_num', column_value, 1);

let $i=180;
while($i>0)
{
    sleep 1;
    let $result = query_get_value(select * from oceanbase.__all_virtual_table where table_name like '%$index_name%', index_status, 1);
    # --echo $result
    if($result == $__index_success_status__)
    {
        let $i = 0;
    }
    dec $i;
}
if($i != -1)
{
    --echo build $index_name failed
}
#sleep 2;
disconnect obsys;
connection default;
--enable_query_log
--enable_result_log
