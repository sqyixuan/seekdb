##########################
#owner group: liboblog
#description:
# 测试点：
# trigger
#
##########################

echo "[CASE_BEGIN] [trigger.inc]";

# 启动oblog
ob1.oblog.gen_conf tb_white_list=oracle.*.* log_level=DEBUG skip_ob_version_compat_check=1;
ob1.oblog.restart;

ob_connect(conn_oracle, ob1.z1.obs0, SYS@oracle, ,SYS, , USE_OB_CONNECTOR);
create user oblog_db identified by "passwd";
ob1.oblog.next;
alter session set current_schema = oblog_db;
sleep 1;

create table employees (pk int, emp_id int, emp_name varchar2(64), dep_id int, salary decimal(10, 2), primary key (pk));
ob1.oblog.next;

create or replace trigger insert_trigger1 before insert or update on employees for each row begin if (:new.salary > 10000) then :new.salary := 10000; end if; end;
ob1.oblog.next;

insert into employees values (1, 10001, 'Alice', 101, 12345);
ob1.oblog.next;

insert into employees values (2, 10002, 'Bob', 101, 8888);
ob1.oblog.next;

drop trigger insert_trigger1;
ob1.oblog.next;

drop table employees;
ob1.oblog.next;

drop user oblog_db cascade;
ob1.oblog.next;

ob1.oblog.stop;

echo "[CASE_END] [trigger.inc]";
