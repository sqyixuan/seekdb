# major freeze currrent tenant
--disable_query_log
--disable_result_log
alter system set ob_compaction_schedule_interval = '3s';
--enable_result_log
--enable_query_log
alter system major freeze;

# wait major freeze success
let $__i__= 100;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__not_finish_tenant_cnt__ = query_get_value(select count(*) as cnt from oceanbase.DBA_OB_MAJOR_COMPACTION where frozen_scn != last_scn, cnt, 1);
    if($__not_finish_tenant_cnt__ > 0)
    {
        let $__i__ = -5;
    }
}