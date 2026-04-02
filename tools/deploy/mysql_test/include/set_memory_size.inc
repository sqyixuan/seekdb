#This is a inc file to set memory_size for shared_storage and shared_nothing scenarios
 
let $startup_mode =query_get_value(show parameters where name = 'ob_startup_mode', value, 1);
if($startup_mode=="shared_storage")
{
  let $memory_size='2G';
}
if($startup_mode!="shared_storage")
{
  let $memory_size='2G';
}