
--disable_query_log
--disable_result_log
let $__i__= 600;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__not_finish_tenant_cnt__ = query_get_value(select count(*) as cnt from oceanbase.DBA_OB_MAJOR_COMPACTION where frozen_scn != last_scn, cnt, 1);
    # all tenant should major merge complete
    if($__not_finish_tenant_cnt__ == 0)
    {
        let $__i__ = -5;
    }
}

if($__i__ != -5)
{
    --echo major freeze failed
    select * from oceanbase.CDB_OB_MAJOR_COMPACTION;
    let $__tenant_id__ = query_get_value(select tenant_id from oceanbase.CDB_OB_MAJOR_COMPACTION where last_scn != frozen_scn, tenant_id, 1);
    let $__frozen_scn__ = query_get_value(select frozen_scn from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $__tenant_id__, frozen_scn, 1);
    eval select * from oceanbase.__all_virtual_server_compaction_event_history where tenant_id = $__tenant_id__ and compaction_scn = $__frozen_scn__;
    select * from oceanbase.__all_virtual_compaction_diagnose_info;
}
sleep 2;

--enable_query_log
--enable_result_log
