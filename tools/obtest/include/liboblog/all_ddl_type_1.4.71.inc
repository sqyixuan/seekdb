##
# owner:千溪
# owner group:liboblog
# description:
# 测试点: 1.4.71支持的DDL语句
#        1. 指定位置加列
#
##

echo "[CASE_BEGIN] [all_ddl_type_1.4.71.inc]";

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

connection conn_obcdc_mysql;
use test;
# 清理数据
--disable_warnings
drop table if exists t1;
--disable_warnings
drop table if exists t2;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.test*.*|*cdc*.*.* skip_ob_version_compat_check=1;
ob1.oblog.restart;

####### 1
echo "case: alter pos column begin";
create table t1(c1 int, c2 int, c3 int, index(c1));
ob1.oblog.next;
insert into t1 values(11, 21, 31);
ob1.oblog.next;

# add first
alter table t1 add column c4 varchar(20) default 'hello_ant' first;
ob1.oblog.next;
insert into t1 values("c4_oblog1", 12, 22, 32);
ob1.oblog.next;

# add before
alter table t1 add column c5 timestamp default '20180314' before c2;
ob1.oblog.next;
insert into t1 values("c4_oblog2", 13, '20180430', 23, 33);
ob1.oblog.next;

# add after
alter table t1 add column c6 datetime(6) default '20180224' after c2;
ob1.oblog.next;
insert into t1 values("c4_oblog3", 14, '20180430', 24, '20180430', 34);
ob1.oblog.next;

####### 2
echo "fulltext index begin";
create table t2 (c1 int, c2 varchar(1000));
ob1.oblog.next;

if ($is_fulltext_index_support == $FULLTEXT_INDEX_SUPPORT)
{
create fulltext index fullidx on t2 (c1, c2) ctxcat(c2);
ob1.oblog.next;
}

replace into t2 values (1, 'oceanbase666'), (2, 'oceanbase666'), (3, 'oceanbase666');
ob1.oblog.next;

# 停止oblog
ob1.oblog.stop;

# 清理数据
--disable_warnings
drop table if exists t1;
--disable_warnings
drop table if exists t2;

echo "[CASE_END] [all_ddl_type_1.4.71.inc]";
