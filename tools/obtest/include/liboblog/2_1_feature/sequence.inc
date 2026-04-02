#
#
# OB_DDL_CREATE_SEQUENCE
# OB_DDL_ALTER_SEQUENCE
# OB_DDL_DROP_SEQUENCE

# 启动oblog
echo "[CASE_BEGIN] [sequence..inc]";
ob1.oblog.gen_conf tb_white_list=oracle.*.* log_level=DEBUG skip_ob_version_compat_check=1;
ob1.oblog.restart;

ob_connect(conn_oracle_obtest,ob1.z1.obs0,OBTEST@oracle,obtest,obtest,,USE_OB_CONNECTOR);
set autocommit=1;
ob_connect(conn_oracle,ob1.z1.obs0, SYS@oracle, ,SYS, , USE_OB_CONNECTOR);
set autocommit=1;
create user oblog_db identified by "passwd";
ob1.oblog.next;
alter session set current_schema = oblog_db;
sleep 1;

create sequence s2x;
ob1.oblog.next;

alter sequence s2x maxvalue 123;
ob1.oblog.next;

create table t1(id int, c1 int);
ob1.oblog.next;

insert into t1(id, c1) values(s2x.nextval, 10);
ob1.oblog.next;

insert into t1(id, c1) values(s2x.nextval, 20);
ob1.oblog.next;

drop sequence s2x;
ob1.oblog.next;

# 删除user
drop user oblog_db cascade;
ob1.oblog.next;

# stop liboblog
ob1.oblog.stop;

echo "[CASE_END] [sequence.inc]";
