connection conn0;

let $query=select count(*) as cnt from (select * from oceanbase.__all_virtual_px_worker_stat group by trace_id) A join oceanbase.__all_virtual_processlist B where A.trace_id = B.trace_id and B.info like '%parallel%';

--let $count=10000
while($count != $wait_count)
{
  let $count = query_get_value($query, cnt, 1);
}
#wait for admission sync
sleep 0.2;