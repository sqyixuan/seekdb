let $__i__ = 60;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__not_equal__ = query_get_value(select count(*) as cnt from oceanbase.__all_virtual_tenant_parameter_stat where name = 'enable_major_freeze' and value != $enable_major_freeze_value, cnt, 1);
    if($__not_equal__ == 0)
    {
        let $__i__ = -5;
    }
}

if($__i__ != -5)
{
    --echo fail to wait set enable_major_freeze = $enable_major_freeze_value
}