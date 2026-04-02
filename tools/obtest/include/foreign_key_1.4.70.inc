let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 启动liboblog
# 关闭校验唯一ID模式
ob1.oblog.set_disable_verify_mode;
ob1.oblog.gen_conf tb_white_list=sys.oblog_db1.*;
ob1.oblog.restart;

connection oblog_conn_sys;

create table t1(pk int, a int, b int, primary key (pk), unique key uk_a_b (b, a));
ob1.oblog.next;
create table t2(pk int, a int, b int, primary key (pk), unique key uk_a_b (a, b), foreign key (a, b) references t1 (b, a) on update CASCADE on delete CASCADE);
ob1.oblog.next;
create table t3(pk int, a int, b int, primary key (pk), unique key uk_a_b (b, a), foreign key (a, b) references t2 (a, b) on update CASCADE on delete CASCADE);
ob1.oblog.next;
create table t4(pk int, a int, b int, primary key (pk), unique key uk_a_b (a, b), foreign key (a, b) references t3 (b, a) on update CASCADE on delete CASCADE);
ob1.oblog.next;
insert into t1 values (10, 11, 12), (20, 21, 22), (30, 31, 32), (40, 41, 42), (50, 51, 52), (60, 61, 62), (70, 71, 72), (80, 81, 82);
ob1.oblog.next;
insert into t2 values (10, 12, 11), (20, 22, 21), (30, 32, 31), (40, 42, 41), (50, 52, 51), (60, 62, 61), (70, 72, 71);
ob1.oblog.next;
insert into t3 values (10, 12, 11), (20, 22, 21), (30, 32, 31), (40, 42, 41), (50, 52, 51), (60, 62, 61);
ob1.oblog.next;
insert into t4 values (10, 11, 12), (20, 21, 22), (30, 31, 32), (40, 41, 42), (50, 51, 52), (60, 61, 62);
ob1.oblog.next;

delete from t1 where pk = 10;
ob1.oblog.next;
update t1 set a = a * 10 where pk = 20;
ob1.oblog.next;
update t2 set a = 82, b = 81 where pk = 30;
ob1.oblog.next;
update t3 set a = 72, b = 71 where pk = 40;
ob1.oblog.next;

select * from t1 order by pk;
select * from t2 order by pk;
select * from t3 order by pk;
select * from t4 order by pk;

--disable_warnings
drop database if exists oblog_db1;
