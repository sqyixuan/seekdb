##########################
#owner group: liboblog
#description:
# 测试点：tablespace
# 1。目前覆盖create/drop
# 2. TODO alter OB还不支持,后面支持
#
##########################

echo "[CASE_BEGIN] [tablespace.inc]";

# 启动oblog
ob1.oblog.gen_conf tb_white_list=oracle.*.* log_level=DEBUG skip_ob_version_compat_check=1;
ob1.oblog.restart;

ob_connect(conn_oracle_obtest,ob1.z1.obs0,OBTEST@oracle,obtest,obtest,,USE_OB_CONNECTOR);
create user oblog_db identified by "passwd";
ob1.oblog.next;
alter session set current_schema = oblog_db;
sleep 1;

# 创建tablespace
create tablespace tp1;
# 输出DDL
ob1.oblog.next;

# 创建table
create table t1(v1 int) tablespace tp1;
ob1.oblog.next;
insert into t1 values(1);
ob1.oblog.next;

# 创建partition
create table test_tp_list_partition(v1 int , v2 int) partition by list(v1) (partition p1 values (1) tablespace tp1,partition p2 values (2) tablespace tp1);
ob1.oblog.next;
insert into test_tp_list_partition values(1,2);
ob1.oblog.next;

create table test_tp_range_partition(v1 int , v2 int) partition by range(v1) (partition p1 values less than (1) tablespace tp1, partition p2 values less than (2) tablespace tp1);
ob1.oblog.next;
insert into test_tp_range_partition values(1,2);
ob1.oblog.next;

set recyclebin=off;
# 删除表
drop table t1;
ob1.oblog.next;

drop table test_tp_list_partition;
ob1.oblog.next;
drop table test_tp_range_partition;
ob1.oblog.next;

drop tablespace tp1;
ob1.oblog.next;

drop user oblog_db cascade;
ob1.oblog.next;

# stop liboblog
ob1.oblog.stop;

echo "[CASE_END] [tablespace.inc]";
