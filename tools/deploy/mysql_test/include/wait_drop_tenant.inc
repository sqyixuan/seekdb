let $__timeout__=120;

while($__timeout__ > 0)
{
    let $__stat__=query_get_value(select count(*) R1 from oceanbase.__all_virtual_unit where status != 'normal',R1,1);
    if($__stat__ < 1)
    {
        --echo check unit status is ok
        let $__timeout__=0;
    }

    dec $__timeout__;

    --real_sleep 1
}
