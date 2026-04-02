 #
 # owner:千溪
 # owner group:liboblog
 # description:
 # 测试点:
 # 验证ddl unique id
 #
 ##

echo "[CASE_BEGIN] [verfify_ddl_unique_id.inc]";

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

--disable_warnings
drop database if exists oblog_db1;
--disable_warnings
drop table if exists t1;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.*.*;
# 开启校验模式
ob1.oblog.set_enable_verify_mode;
ob1.oblog.restart;

# sys租户下操作
connection oblog_conn_sys;
let $clusterID = query_get_value(show parameters like 'cluster_id', value, 1);
set recyclebin=on;

create database oblog_db1;
let $tenantID = query_get_value(select exec_tenant_id from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1, exec_tenant_id, 1);
let $unique_id = query_get_value(select concat($clusterID, "_", schema_version) as unique_id from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1, unique_id, 1);
ob1.oblog.next_with_unique_id $unique_id;

drop database oblog_db1;
let $unique_id = query_get_value(select concat($clusterID, "_", schema_version) as unique_id from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1, unique_id, 1);
ob1.oblog.next_with_unique_id $unique_id;
eval select count(*) from oceanbase.__all_recyclebin where object_name = (select concat("__recycle_\$_", $clusterID, "_", $tenantID, "_", schema_version) from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1) order by gmt_create desc limit 1;

# table
create table t1 (pk int primary key);
let $unique_id = query_get_value(select concat($clusterID, "_", schema_version) as unique_id from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1, unique_id, 1);
ob1.oblog.next_with_unique_id $unique_id;

# view
create view t1_view (pk) as select pk from t1;
let $unique_id = query_get_value(select concat($clusterID, "_", schema_version) as unique_id from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1, unique_id, 1);
ob1.oblog.next_with_unique_id $unique_id;

drop view t1_view;
let $unique_id = query_get_value(select concat($clusterID, "_", schema_version) as unique_id from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1, unique_id, 1);
ob1.oblog.next_with_unique_id $unique_id;
eval select count(*) from oceanbase.__all_recyclebin where object_name = (select concat("__recycle_\$_", $clusterID, "_", $tenantID, "_", schema_version) from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1) order by gmt_create desc limit 1;

drop table t1;
let $unique_id = query_get_value(select concat($clusterID, "_", schema_version) as unique_id from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1, unique_id, 1);
ob1.oblog.next_with_unique_id $unique_id;
eval select count(*) from oceanbase.__all_recyclebin where object_name = (select concat("__recycle_\$_", $clusterID, "_", $tenantID, "_", schema_version) from oceanbase.__all_ddl_operation where ddl_stmt_str != '' order by schema_version desc limit 1) order by gmt_create desc limit 1;

# 停止oblog
ob1.oblog.stop;

# 清理数据
--disable_warnings
drop database if exists oblog_db1;
--disable_warnings
drop table if exists t1;

echo "[CASE_END] [verfify_ddl_unique_id.inc]";
