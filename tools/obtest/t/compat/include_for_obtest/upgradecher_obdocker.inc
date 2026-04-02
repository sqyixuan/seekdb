let $count = 5;
while ($count > 0)
{
   let $upgrade_checker=deploy_get_value(obdocker.upgrade_check_before);
   if ($upgrade_checker == true)
   {
        let $count = -6;
    }
   if ($upgrade_checker == false)
   {
        dec $count;
        sleep 15;
   }
}
if($count == 0)
{
   --exit
}
