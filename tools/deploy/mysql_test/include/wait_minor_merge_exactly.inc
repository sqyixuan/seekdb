# set $table_name outside
# set $tenant_name outside
# set $minor_compact_trigger

--disable_query_log
--disable_result_log

connect (obsys_minor,$OBMYSQL_MS0,admin,$OBMYSQL_PWD,oceanbase,$OBMYSQL_PORT);
connection obsys_minor;

let $minor_type_no = 11;
let $mini_type_no = 12; 
let $tablet_id = query_get_value(select tablet_id from __all_virtual_table where table_name = $table_name, tablet_id, 1);
let $leader_ip = query_get_value(select svr_ip from __all_virtual_server_stat, svr_ip, 1);
let $leader_port = query_get_value(select svr_port from __all_virtual_server_stat, svr_port, 1);
let $mini_cnt  = query_get_value(select count(*) as mini_cnt  from __all_virtual_table_mgr where tablet_id = $tablet_id and table_type = $mini_type_no,  mini_cnt, 1);
let $minor_cnt = query_get_value(select count(*) as minor_cnt from __all_virtual_table_mgr where tablet_id = $tablet_id and table_type = $minor_type_no, minor_cnt, 1);
let $total_cnt = query_get_value(select count(*) as total_cnt from __all_virtual_table_mgr where tablet_id = $tablet_id and (table_type = $mini_type_no or table_type = $minor_type_no), total_cnt, 1);

inc $total_cnt;
# only mini sstable
if ($total_cnt <= $minor_compact_trigger)
{
    --echo mini_cnt is $mini_cnt, minor_cnt is $minor_cnt, minor_compact_trigger is $minor_compact_trigger, only wait for mini 
    eval alter system minor freeze tenant = $tenant_name;
    disconnect obsys_minor;
    --source wait_minor_merge.inc
}

# can trigger minor sstable
if ($total_cnt > $minor_compact_trigger)
{
    --echo mini_cnt is $mini_cnt, minor_cnt is $minor_cnt, minor_compact_trigger is $minor_compact_trigger, wait for minor
    let $i = 360;
    if ($minor_cnt == 0)
    {
        eval alter system minor freeze tenant = $tenant_name;
        sleep 5;
        while ($i > 0)
        {
            sleep 1;
            dec $i;
            let $minor_cnt = query_get_value(select count(*) as minor_cnt from __all_virtual_table_mgr where tablet_id = $tablet_id and table_type = $minor_type_no, minor_cnt, 1);
            if ($minor_cnt != 0) {
                let $i = -1;
            }
        }

        if ($i != -1)
        {
            --echo wait minor merge exactly failed
        }
    }

    if ($minor_cnt != 0)
    {
        let $old_scn = query_get_value(select END_LOG_SCN as old_scn from __all_virtual_table_mgr where tablet_id = $tablet_id and table_type = $minor_type_no, old_scn, 1);
        eval alter system minor freeze tenant = $tenant_name;
        sleep 5;
        while ($i > 0)
        {
            sleep 1;
            dec $i;
            let $new_minor_cnt = query_get_value(select count(*) as minor_cnt from __all_virtual_table_mgr where tablet_id = $tablet_id and table_type = $minor_type_no and END_LOG_SCN != $old_scn, minor_cnt, 1);
            if ($new_minor_cnt != 0) {
                let $i = -1;
            }
        }

        if ($i != -1)
        {
            --echo wait minor merge exactly failed
        }
    }
    disconnect obsys_minor;
}

connection default;

--enable_query_log
--enable_result_log
