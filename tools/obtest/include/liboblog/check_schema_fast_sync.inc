ob_connect(ob_sys_check_schema, ob1.z1.obs0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);
connection ob_sys_check_schema;

let $is_rs = query_get_value(select count(*) as cnt from  oceanbase.__all_server where svr_ip='$OBMYSQL_MS0' and inner_port=$OBMYSQL_PORT and with_rootserver=1, cnt, 1);
if ($is_rs == 0)
{
    let $i = 10;
    while($i > 0)
    {
        sleep 1;
        let $cnt = query_get_value(select count(*) as cnt from oceanbase.__all_virtual_server_schema_info as a join oceanbase.__all_virtual_server_schema_info as b on a.tenant_id = b.tenant_id  where a.refreshed_schema_version != b.refreshed_schema_version, cnt, 1);
        dec $i;
        if ($cnt == 0)
        {
            let $i = -4;
        }
    }
    if ($i != -4)
    {
        --echo check schema sync timeout
        eval select * from oceanbase.__all_virtual_server_schema_info;
    }
}

