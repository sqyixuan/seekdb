ob1.check_until_timeout select count(*) from oceanbase.CDB_OB_MAJOR_COMPACTION where LAST_SCN != GLOBAL_BROADCAST_SCN , 1, 100;

--disable_query_log
--disable_result_log
let $__i__= 1000;
while($__i__ > 0)
{
    sleep 1;
    dec $__i__;
    let $__not_finish_tenant_cnt__ = deploy_get_value(ob1.select select count(*) as cnt from oceanbase.CDB_OB_MAJOR_COMPACTION where frozen_scn != last_scn, cnt, 1);
    # all tenant should major merge complete
    if($__not_finish_tenant_cnt__ == 0)
    {
        let $__i__ = -5;
    }
}

--enable_query_log
--enable_result_log

if($__i__ != -5)
{
    --echo major freeze failed
    ob1.mysql select * from oceanbase.CDB_OB_MAJOR_COMPACTION;
    let $__tenant_id__ = deploy_get_value(ob1.select select tenant_id from oceanbase.CDB_OB_MAJOR_COMPACTION where LAST_SCN != GLOBAL_BROADCAST_SCN, tenant_id, 1);
    let $__compaction_scn__ = deploy_get_value(ob1.select select FROZEN_SCN from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id = $__tenant_id__, FROZEN_SCN, 1);
    let $__svr_ip__ = deploy_get_value(ob1.z1.obs0.ip);
    let $__svr_port__ = deploy_get_value(ob1.z1.obs0.mysql_port);

    connect (inner_obsys, $__svr_ip__, root@sys, , oceanbase, $__svr_port__);
    connection inner_obsys; 
    eval select * from oceanbase.__all_virtual_shared_storage_compaction_info where tenant_id = $__tenant_id__ and tablet_id=0 and ls_id > 0 order by ls_id,obj_type,svr_port asc;
    eval select * from oceanbase.__all_virtual_server_compaction_event_history where tenant_id = $__tenant_id__ and compaction_scn = $__compaction_scn__ order by event_timestamp asc;
    eval select * from oceanbase.__all_virtual_compaction_diagnose_info;
}

