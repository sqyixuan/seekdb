##
--disable_query_log
--disable_result_log

connect (obsys,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys;

let $index_status_expect = 2;

let $i=180;
while($i>0)
{
    sleep 1;
    #--echo $index_name
    let $result = query_get_value(select * from oceanbase.__all_virtual_table where table_name='$index_name', index_status, 1);
    # --echo $result
    if($result == $index_status_expect)
    {
        let $i = 0;
    }
    dec $i;
}
sleep 2;

if($i != -1)
{
    --echo build $index_name failed
}

disconnect obsys;
connection default;

--enable_query_log
--enable_result_log
