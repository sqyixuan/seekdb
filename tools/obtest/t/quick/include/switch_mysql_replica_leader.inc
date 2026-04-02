#############################################
#owner : @xianlin.lh
#owner group: clog
#description: quick test
# 测试点：验证mysql租户下的switch replica leader
#############################################
connection conn_sys;

let $ls_id = deploy_get_value(ob1.select select ls_id from __all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id != 1 order by ls_id limit 1, ls_id, 1);
let $leader_svr_ip = deploy_get_value(ob1.select select svr_ip from __all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $ls_id and role = 1, svr_ip, 1);
let $leader_svr_port = deploy_get_value(ob1.select select svr_port from __all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $ls_id and role = 1, svr_port, 1);
let $follower1_svr_ip = deploy_get_value(ob1.select select svr_ip from __all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $ls_id and role = 2 order by svr_ip, svr_port limit 1, svr_ip, 1);
let $follower1_svr_port = deploy_get_value(ob1.select select svr_port from __all_virtual_ls_meta_table where tenant_id = $mysql_tenant_id and ls_id = $ls_id and role = 2 order by svr_ip, svr_port limit 1, svr_port, 1);

--disable_query_log
eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$follower1_svr_ip' and svr_port='$follower1_svr_port' and ls_id = $ls_id and role = 1;
eval alter system switch replica leader ls=$ls_id server='$follower1_svr_ip:$follower1_svr_port' tenant='mysql';
ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$follower1_svr_ip' and svr_port='$follower1_svr_port' and ls_id = $ls_id and role = 1, 1, 6;
eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$follower1_svr_ip' and svr_port='$follower1_svr_port' and ls_id = $ls_id and role = 1;
eval alter system switch replica leader ls=$ls_id server='$leader_svr_ip:$leader_svr_port' tenant='mysql';
ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$leader_svr_ip' and svr_port='$leader_svr_port' and ls_id = $ls_id and role = 1, 1, 6;
eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$leader_svr_ip' and svr_port='$leader_svr_port' and ls_id = $ls_id and role = 1;
--enable_query_log

# begin; insert ; switch replica leader; commit;
connection conn_mysql;

create table if not exists t_for_switch (c1 int);
source include/check_schema_sync.inc;

begin;
insert into t_for_switch values (1);

connection conn_sys;
--disable_query_log
eval alter system switch replica leader ls=$ls_id server='$follower1_svr_ip:$follower1_svr_port' tenant='mysql';
ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$follower1_svr_ip' and svr_port='$follower1_svr_port' and ls_id = $ls_id and role = 1, 1, 6;
eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$follower1_svr_ip' and svr_port='$follower1_svr_port' and ls_id = $ls_id and role = 1;
--enable_query_log

connection conn_mysql;

commit;

select * from t_for_switch order by c1 asc;

connection conn_sys;

--disable_query_log
eval alter system switch replica leader ls=$ls_id server='$leader_svr_ip:$leader_svr_port' tenant='mysql';
ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$leader_svr_ip' and svr_port='$leader_svr_port' and ls_id = $ls_id and role = 1, 1, 6;
eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$leader_svr_ip' and svr_port='$leader_svr_port' and ls_id = $ls_id and role = 1;
--enable_query_log

# begin; insert; switch replica leader; insert; commit;
connection conn_mysql;

begin;
insert into t_for_switch values (2);

connection conn_sys;
--disable_query_log
eval alter system switch replica leader ls=$ls_id server='$follower1_svr_ip:$follower1_svr_port' tenant='mysql';
ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$follower1_svr_ip' and svr_port='$follower1_svr_port' and ls_id = $ls_id and role = 1, 1, 6;
eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$follower1_svr_ip' and svr_port='$follower1_svr_port' and ls_id = $ls_id and role = 1;
--enable_query_log

connection conn_mysql;

insert into t_for_switch values (3);

commit;

select * from t_for_switch order by c1 asc;

connection conn_sys;

--disable_query_log
eval alter system switch replica leader ls=$ls_id server='$leader_svr_ip:$leader_svr_port' tenant='mysql';
ob1.check_until_timeout select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$leader_svr_ip' and svr_port='$leader_svr_port' and ls_id = $ls_id and role = 1, 1, 6;
eval select count(*) from __all_virtual_ls_meta_table where tenant_id = '$mysql_tenant_id' and svr_ip = '$leader_svr_ip' and svr_port='$leader_svr_port' and ls_id = $ls_id and role = 1;
--enable_query_log

connection conn_mysql;
set recyclebin = off;
DROP TABLE IF EXISTS t_for_switch;