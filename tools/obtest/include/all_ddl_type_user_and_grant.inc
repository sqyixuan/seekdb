 #
 # owner:修铭
 # owner group:liboblog
 # description:
 # 测试点: 测试用户和权限相关DDL
 # 注意：1.4.4之后才支持输出用户和权限相关DDL语句
 ##

echo "[CASE_BEGIN] [all_ddl_type_user_and_grant.inc]";

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.*.* skip_ob_version_compat_check=1;
ob1.oblog.restart;

# 创建用户: OB_DDL_CREATE_USER
create user test_user1 identified by '123456';
ob1.oblog.next;

# 重命名用户: OB_DDL_RENAME_USER
# FIXME: 其他重命名方式
rename user test_user1 to test_user3;
ob1.oblog.next;
rename user test_user3 to test_user1;
ob1.oblog.next;

# 设置密码
# OB_DDL_SET_PASSWD
set password for test_user1 = password('abc');
ob1.oblog.next;

# 加锁用户
# OB_DDL_LOCK_USER
alter user test_user1 account lock;
ob1.oblog.next;

# 添加表权限：OB_DDL_GRANT_REVOKE_TABLE
grant drop on oceanbase.__all_ddl_operation to test_user1;
ob1.oblog.next;

# 删除表权限：OB_DDL_DEL_TABLE_PRIV
revoke drop on oceanbase.__all_ddl_operation from test_user1;
ob1.oblog.next;

# 添加DB权限: OB_DDL_GRANT_REVOKE_DB
grant drop on oceanbase.* to test_user1;
ob1.oblog.next;

# 删除DB权限：OB_DDL_DEL_DB_PRIV
revoke drop on oceanbase.* from test_user1;
ob1.oblog.next;

# 添加所有权限: OB_DDL_GRANT_REVOKE_USER
grant drop on *.* to test_user1;
ob1.oblog.next;

# 删除所有权限：OB_DDL_GRANT_REVOKE_USER
revoke drop on *.* from test_user1;
ob1.oblog.next;

# 删除用户
# OB_DDL_DROP_USER
drop user test_user1;
ob1.oblog.next;

# 停止oblog
ob1.oblog.stop;

echo "[CASE_END] [all_ddl_type_user_and_grant.inc]";
