--disable_query_log
alter system major freeze;
let $__i__= 600;
while($__i__ > 0)
{
  sleep 1;
  dec $__i__;
  let $__frozen_scn__ = query_get_value(select /*+query_timeout(100000000)*/ frozen_scn from oceanbase.DBA_OB_MAJOR_COMPACTION, frozen_scn, 1);
  let $__merged_version__ = query_get_value(select /*+query_timeout(100000000)*/ last_scn from oceanbase.DBA_OB_MAJOR_COMPACTION, last_scn, 1); 
  if($__frozen_scn__ == $__merged_version__)
  {
    let $__i__ = -5;
  }
}
if($__i__ != -5)
{
  --echo major freeze failed
}
sleep 5;
--enable_query_log
