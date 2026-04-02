#
# owner:千溪
# owner group:liboblog
# description:
# 测试点:
#   动态增加分区，该分区插入若干条数据, 测试回拉历史场景, 保证该动态添加分区的数据都能迭代到
#   bug: 
#
##

connection oblog_conn_tt;
--disable_warnings
drop database if exists oblog_db;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=oblog_tt.*.*;
ob1.oblog.restart;

# 创建database
create database oblog_db;
ob1.oblog.next;
use oblog_db;

let $start_tstamp2 = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);
let $DML_COUNT=0;

################### 一级range分区 ###################
#  新创建表pkey其partition_cnt默认均为0，新增分区只递增partition_idx
#  老版本pkey，假设partition_cnt为6, 新增分区保持parition_cnt值不变，仍为6
create table t1(c1 int, c2 int, primary key(c1,c2)) partition by range(c1) (partition p0 values less than(10), partition p1 values less than(20));
ob1.oblog.next;

# 为t1表动态增加1个range分区
alter table t1 add partition (partition p2 values less than(30));
ob1.oblog.next;

# 在该分区上插入数据，并重复修改同一行, 保证回拉历史DML语句的顺序性
insert into t1 values (21, 1);
ob1.oblog.next;
inc $DML_COUNT;

update t1 set c2 = 2 where c1 = 21 and c2 = 1;
ob1.oblog.next;
inc $DML_COUNT;

update t1 set c2 = 3 where c1 = 21 and c2 = 2;
ob1.oblog.next;
inc $DML_COUNT;

update t1 set c2 = 4 where c1 = 21 and c2 = 3;
ob1.oblog.next;
inc $DML_COUNT;

update t1 set c2 = 5 where c1 = 21 and c2 = 4;
ob1.oblog.next;
inc $DML_COUNT;

update t1 set c2 = 6 where c1 = 21 and c2 = 5;
ob1.oblog.next;
inc $DML_COUNT;

# t1表动态删除range分区
alter table t1 drop partition (p0, p1);
ob1.oblog.next;

# stop liboblog
ob1.oblog.stop;

# 启动liboblog, 启动时间戳为历史位点
ob1.oblog.set_start_tstamp $start_tstamp2;
ob1.oblog.gen_conf tb_white_list=oblog_tt.*.*;
ob1.oblog.restart;

echo $DML_COUNT;
let $count1 = $DML_COUNT;

# 迭代table t1
while ($count1 > 0)
{
  ob1.oblog.next_dml;
  dec $count1;
}

# stop liboblog
ob1.oblog.stop;

# 清理数据
--disable_warnings
drop database if exists oblog_db;
