--disable_query_log
--disable_result_log
eval $select_example;
eval $query_stmt; 
let $select_stmt=select found_rows() as expect;
let $result=query_get_value($select_stmt, expect, 1);
if($result==$expect)
{
  echo '*********$query_stmt do nothing for found_rows';
}
if($result!=$expect)
{
  #echo $query_stmt do affected found_rows;;
}
--enable_result_log
--enable_query_log
