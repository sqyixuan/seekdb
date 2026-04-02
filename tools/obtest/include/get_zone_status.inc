let $zone_status = deploy_get_value(ob1.select select value from __all_zone where name = 'status' and zone = '$zone', value, 1);
if ($zone_status == 2)
{
  echo zone is active;
}
if ($zone_status == 1)
{
  echo zone is inactive;
}
