connection conn0;

let $query=select sum(peer_target_used) as cnt from oceanbase.__all_virtual_px_target_monitor;

--let $sum=10000
while($sum != 0)
{
  let $sum = query_get_value($query, cnt, 1);
}