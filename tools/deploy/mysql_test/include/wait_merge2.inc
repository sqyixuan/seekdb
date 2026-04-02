--disable_query_log
--disable_result_log

let $__i__= 180;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__global_stat__ = query_get_value(select comment from oceanbase.__all_cluster_stat where name='global_stat',comment, 1);
    if($__global_stat__ == NORMAL)
    {
        let $__i__ = -5;
    }
}

if($__i__ != -5)
{
    --echo major freeze failed
}
sleep 2;

--enable_query_log
--enable_result_log
