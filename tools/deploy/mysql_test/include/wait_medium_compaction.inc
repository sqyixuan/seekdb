connect (obsys_ydm,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,test,$OBMYSQL_PORT);
connection obsys_ydm;

let $__i__= 600;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__frozen_scn__ = query_get_value(select sum(frozen_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    let $__merged_version__ = query_get_value(select sum(last_scn/1000) as sum_value from oceanbase.CDB_OB_MAJOR_COMPACTION, sum_value, 1);
    if($__frozen_scn__ == $__merged_version__)
    {
        let $__i__ = -5;
    }
}

if($__i__ != -5)
{
    --echo major freeze failed
}

let $__i__= 300;
while ($__i__ > 0)
{
  dec $__i__;
  sleep 1;
  connection conn0;
  let $unfinished_count = query_get_value(select count(*) as c from oceanbase.__all_virtual_tablet_meta_table where status != 0 or report_scn < compaction_scn, c, 1);
  if ($unfinished_count == 0)
  {
    let $__i__ = -5;
  }
}

disconnect obsys_ydm;
connection default;
