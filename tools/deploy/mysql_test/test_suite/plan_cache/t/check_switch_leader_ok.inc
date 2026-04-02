--disable_query_log
--disable_result_log

let $max_count = 100;
while ($max_count > 0)
{
  dec $max_count;
  --error 0, 600
  let $v = query_get_value(select count(*) from t1, count(*), 1);
  if ($v != 0)
  {
    let $max_count = 0;
  }
}

--enable_query_log
--enable_result_log
