##########################
#owner :  千溪
#owner group: liboblog
#description:
# 测试点：
# label security
#
##########################

# 启动oblog
echo "[CASE_BEGIN] [label.inc]";

ob_connect(conn_oracle, ob1, SYS@oracle, ,SYS, , USE_OB_CONNECTOR);
alter user LBACSYS account unlock;
alter user LBACSYS IDENTIFIED BY LBACSYS;
ob1.oblog.gen_conf tb_white_list=oracle.*.* log_level=DEBUG;
ob1.oblog.restart;

# 创建t1
create table t1 (c1 int);
# 输出DDL
ob1.oblog.next;

#ob_connect(conn_labcsys,ob1,LBACSYS@oracle,LBACSYS,LBACSYS,,USE_OB_CONNECTOR);
ob_connect(conn_labcsys,ob1,LBACSYS@oracle,,LBACSYS,,USE_OB_CONNECTOR);

# 创建policy
CALL SA_SYSDBA.CREATE_POLICY('MY_POLICY', 'MY_LABEL', '');
ob1.oblog.next;

# 创建level
CALL SA_COMPONENTS.CREATE_LEVEL('MY_POLICY', 100, 'PUBLIC', 'Public Level');
ob1.oblog.next;
CALL SA_COMPONENTS.CREATE_LEVEL('MY_POLICY', 200, 'INTERNAL', 'Internal Level');
ob1.oblog.next;
CALL SA_COMPONENTS.CREATE_LEVEL('MY_POLICY', 300, 'HIGH', 'High Level');
ob1.oblog.next;
CALL SA_COMPONENTS.CREATE_LEVEL('MY_POLICY', 400, 'VERY HIGH', 'Very High Level');
ob1.oblog.next;
CALL SA_COMPONENTS.CREATE_LEVEL('MY_POLICY', 500, 'TOP', 'Top Level');
ob1.oblog.next;

# 修改level
CALL SA_COMPONENTS.ALTER_LEVEL('MY_POLICY', 400, 'VERY VERY HIGH', 'Very Very High Level');
ob1.oblog.next;

#创建label
CALL SA_LABEL_ADMIN.CREATE_LABEL('MY_POLICY', 10000, 'PUBLIC', 1);
ob1.oblog.next;
CALL SA_LABEL_ADMIN.CREATE_LABEL('MY_POLICY', 20000, 'INTERNAL', 1);
ob1.oblog.next;
CALL SA_LABEL_ADMIN.CREATE_LABEL('MY_POLICY', 30000, 'HIGH', 1);
ob1.oblog.next;
CALL SA_LABEL_ADMIN.CREATE_LABEL('MY_POLICY', 40000, 'VERY VERY HIGH', 1);
ob1.oblog.next;

#修改label
CALL SA_LABEL_ADMIN.ALTER_LABEL('MY_POLICY', 40000, 'TOP', 1);
ob1.oblog.next;

#删除label
CALL SA_LABEL_ADMIN.DROP_LABEL('MY_POLICY', 40000);
ob1.oblog.next;

# 删除level
CALL SA_COMPONENTS.DROP_LEVEL('MY_POLICY', 400);
ob1.oblog.next;

#指定用户label
CALL SA_USER_ADMIN.SET_LEVELS('MY_POLICY', 'LBACSYS', 'HIGH', 'PUBLIC', 'PUBLIC', 'PUBLIC');
ob1.oblog.next;

#应用policy到表
CALL SA_POLICY_ADMIN.APPLY_TABLE_POLICY('MY_POLICY', 'OBLOG_DB', 'T1', '', '', '');
ob1.oblog.next;

#设置session label（主体标签）
CALL SA_SESSION.SET_LABEL('MY_POLICY', 'INTERNAL');
CALL SA_SESSION.SET_ROW_LABEL('MY_POLICY', 'INTERNAL');
select SA_SESSION.LABEL('MY_POLICY') RET from dual;
select SA_SESSION.ROW_LABEL('MY_POLICY') RET from dual;

#向表中插入数据（客体标签）
insert into t1 values(1, 10000);
ob1.oblog.next;
insert into t1(MY_LABEL) values(20000);
ob1.oblog.next;
insert into t1(c1) values (2);
ob1.oblog.next;
select * from t1;

#改变session label（主体标签）
CALL SA_SESSION.SET_LABEL('MY_POLICY', 'PUBLIC');
CALL SA_SESSION.SET_ROW_LABEL('MY_POLICY', 'PUBLIC');
select * from t1;

#删除策略，相关label和level会级联删除
CALL SA_SYSDBA.DROP_POLICY('MY_POLICY', 1);
ob1.oblog.next;

# 删除表
drop table t1;
# 输出DDL
ob1.oblog.next;

# stop liboblog
ob1.oblog.stop;
echo "[CASE_END] [label.inc]";
