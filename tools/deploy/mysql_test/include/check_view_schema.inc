--disable_query_log
if ($target_status == '')
{
    let $target_status = VALID;
}
let $i = 100;
while($i > 0)
{
    sleep 2;
    let $status = query_get_value($query, STATUS, 1);
    dec $i;
    if ($status == $target_status)
    {
        let $i = -1;
    }
}
--enable_query_log
