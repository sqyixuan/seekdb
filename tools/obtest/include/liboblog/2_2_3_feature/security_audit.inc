##########################
#
#owner group: liboblog
#description:
# 测试点：
# security audit
#

##########################

echo "[CASE_BEGIN] [audit.inc]";
ob_connect(conn_oracle, ob1.z1.obs0, SYS@oracle, ,SYS, , USE_OB_CONNECTOR);
set autocommit = 1;
alter user ORAAUDITOR account unlock;
alter user ORAAUDITOR IDENTIFIED BY ORAAUDITOR;

# 启动oblog
ob1.oblog.gen_conf tb_white_list=oracle.*.* log_level=DEBUG skip_ob_version_compat_check=1;
ob1.oblog.restart;

create user oblog_db identified by "passwd";
ob1.oblog.next;
alter session set current_schema = oblog_db;
sleep 1;

if ($is_equal_or_greater_than_227 != $IS_EQUAL_OR_GREATER_THAN_227)
{
ob_connect(conn_oracle,ob1.z1.obs0,ORAAUDITOR@oracle,,oblog_db,,USE_OB_CONNECTOR);
}

if ($is_equal_or_greater_than_227 == $IS_EQUAL_OR_GREATER_THAN_227)
{
ob_connect(conn_oracle,ob1.z1.obs0,ORAAUDITOR@oracle,ORAAUDITOR,oblog_db,,USE_OB_CONNECTOR);
}

alter session set current_schema = oblog_db;

alter system set audit_sys_operations = 'true';
alter system set audit_trail = 'DB,EXTENDED';
set @@recyclebin=OFF;
set @@ob_enable_plan_cache=OFF;
set @@ob_query_timeout=10 * @@ob_query_timeout;

#all_virtual_security_audit_agent已废弃
#select TENANT_ID, bitand(AUDIT_ID, 1099511627775) AUDIT_ID, AUDIT_TYPE, bitand(OWNER_ID, 1099511627775) OWNER_ID, OPERATION_TYPE, IN_SUCCESS, IN_FAILURE from sys.all_virtual_security_audit_agent;

create sequence s1;
ob1.oblog.next;

create table t1 (pk int primary key, a int);
ob1.oblog.next;

insert into t1 values (s1.nextval, 1);
ob1.oblog.next;

audit all statements by ORAAUDITOR;
ob1.oblog.next;

audit all statements by SYS WHENEVER SUCCESSFUL;
ob1.oblog.next;

audit all statements;
ob1.oblog.next;

#select TENANT_ID, bitand(AUDIT_ID, 1099511627775) AUDIT_ID, AUDIT_TYPE, bitand(OWNER_ID, 1099511627775) OWNER_ID, OPERATION_TYPE, IN_SUCCESS, IN_FAILURE from sys.all_virtual_security_audit_agent;

insert into t1 values (s1.nextval, 2);
ob1.oblog.next;

noaudit all statements by ORAAUDITOR;
ob1.oblog.next;

noaudit all statements by SYS WHENEVER SUCCESSFUL;
ob1.oblog.next;

noaudit all statements;
ob1.oblog.next;

drop user oblog_db cascade;
ob1.oblog.next;

# stop liboblog
ob1.oblog.stop;

echo "[CASE_END] [audit.inc]";
