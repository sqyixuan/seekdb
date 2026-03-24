#命令重试
while ($count > 0)
{
   let $result_checker=deploy_get_value($retry_block);
   if ($result_checker == true)
   {
        let $count = -1;
    }
   if ($result_checker == false)
   {
        dec $count;
        sleep 60;
   }
}
if($count == 0)
{
   --exit
}
