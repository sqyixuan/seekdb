let $__timeout__=120;

while($__timeout__ > 0)
{
    let $__stat__=query_get_value(select * from __updateserver_memtable_info where log_str='$memtable',table_status,1);
    if($__stat__ == $table_status)
    {
        --echo check  status => $table_status is ok
        let $__timeout__=0;
    }

    dec $__timeout__;

    --real_sleep 1
}
