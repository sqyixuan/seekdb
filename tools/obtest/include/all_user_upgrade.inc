#group:sql1
#owner:yangze.yz
#当在内部表中添加权限后需要保持beta版的兼容，因为需要添加列，同时对之前具有all privileges的用户添加PROCESS权限

alter table oceanbase.__all_user add column priv_process bigint(20) not null default 0;
alter table oceanbase.__all_user_history add column priv_process bigint(20) default 0;
let $tenant_cnt = query_get_value(select count(*) as cnt from oceanbase.__all_tenant, cnt, 1);
while ($tenant_cnt)
{
  let $tenant_id = query_get_value(select tenant_id from oceanbase.__all_tenant order by tenant_id, tenant_id, $tenant_cnt);
  eval update oceanbase.__all_user_history set priv_process = 1 where priv_alter = 1 and priv_create = 1 and priv_delete = 1 and priv_drop = 1 and priv_insert = 1 and priv_update = 1 and priv_select = 1 and priv_index = 1 and priv_create_view = 1 and priv_show_view =1 and priv_show_db = 1 and priv_create_user =1 and priv_super = 1 and tenant_id = $tenant_id;
  eval update oceanbase.__all_user set priv_process = 1 where priv_alter = 1 and priv_create = 1 and priv_delete = 1 and priv_drop = 1 and priv_insert = 1 and priv_update = 1 and priv_select = 1 and priv_index = 1 and priv_create_view = 1 and priv_show_view =1 and priv_show_db = 1 and priv_create_user =1 and priv_super = 1 and tenant_id = $tenant_id;
  dec $tenant_cnt;
}
