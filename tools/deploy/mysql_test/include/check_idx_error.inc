##
--disable_query_log
--disable_result_log

let $index_status_expect = 2;
let $result = 2;

connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;
let $i=180;
while($i>0)
{
    sleep 1;
    #--echo $index_name
    let $result = query_get_value(select count(*) from oceanbase.__all_virtual_table where table_name like '__idx_%' and index_status=5,count(*),1);
    if($result > 0)
    {
        let $i = -4;
    }
    dec $i;
}
sleep 2;

if($i == -4)
{
    --echo idx  data error
}
disconnect obsys;
connection default;
--enable_query_log
--enable_result_log
