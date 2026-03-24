#
# owner:千溪
# owner group:liboblog
# description: 全局索引表建表语句
#

let $TABLE_NUMBER = 11;
let $DML_CNT = 0;
let $DDL_CNT = 0;

############## 一 创建带有全局普通索引的主表
create table t1 (c1 int, c2 int, c3 int, c4 int, index (c2,c3) global, primary key (c1)) partition by hash(c1) partitions 4;
inc $DDL_CNT;

############## 二 创建带有全局唯一索引的主表
create table t2 (c1 int, c2 int, c3 int, c4 int, unique index (c2,c3) global, primary key (c1)) partition by hash(c1) partitions 4;
inc $DDL_CNT;

############## 三 创建主表, 再创建全局普通索引
# 3.1 全局索引无独立分区, 如下eg: 主表单分区, 全局索引表未指定分区方式无独立分区
create table t3 (c1 int, c2 int, c3 int, c4 int, primary key (c1));
inc $DDL_CNT;
# OB_DDL_CREATE_INDEX(20), 非OB_DDL_CREATE_GLOBAL_INDEX(40)
create index g_normal_key_t3 on t3(c2,c3,c4) global;
inc $DDL_CNT;

# 3.2 全局普通索引有独立分区, 如下eg: 主表单分区, 全局索引表指定分区方式有多个分区
create table t4 (c1 int, c2 int, c3 int, c4 int, primary key (c1));
inc $DDL_CNT;
create index g_normal_key_t4 on t4(c2,c3,c4) global partition by hash(c2) partitions 5;
inc $DDL_CNT;

# 3.3 全局普通索引有一个分区, 如下eg: 主表4个分区, 全局索引表有1个分区
create table t5 (c1 int, c2 int, c3 int, c4 int, primary key (c1)) partition by hash(c1) partitions 4;
inc $DDL_CNT;
create index g_normal_key_t5 on t5(c2,c3,c4) global;
inc $DDL_CNT;

# 3.4 全局索引有多个分区, 如下eg: 主表4个分区, 全局索引表有5个分区
create table t6 (c1 int, c2 int, c3 int, c4 int, primary key (c1)) partition by hash(c1) partitions 4;
inc $DDL_CNT;
create index g_normal_key_t6 on t6(c2,c3,c4) global partition by hash(c2) partitions 5;
inc $DDL_CNT;

############## 四 创建主表, 再创建全局唯一索引
# 4.1 全局唯一索引无独立分区, 如下eg: 主表单分区, 全局索引表未指定分区方式无独立分区
create table t7 (c1 int, c2 int, c3 int, c4 int, primary key (c1));
inc $DDL_CNT;
# OB_DDL_CREATE_INDEX(20), 非OB_DDL_CREATE_GLOBAL_INDEX(40)
create unique index g_unique_gkey_t7 on t7(c2,c3,c4) global;
inc $DDL_CNT;

# 4.2 全局唯一索引有独立分区, 如下eg: 主表单分区, 全局索引表指定分区方式有多个分区
create table t8 (c1 int, c2 int, c3 int, c4 int, primary key (c1));
inc $DDL_CNT;
create unique index g_unique_gkey_t8 on t8(c2,c3,c4) global partition by hash(c2) partitions 5;
inc $DDL_CNT;

# 4.3 全局唯一索引有一个分区, 如下eg: 主表4个分区, 全局索引表有1个分区
create table t9 (c1 int, c2 int, c3 int, c4 int, primary key (c1)) partition by hash(c1) partitions 4;
inc $DDL_CNT;
create unique index g_unique_gkey_t9 on t9(c2,c3,c4) global;
inc $DDL_CNT;

# 4.4 全局索引有多个分区, 如下eg: 主表4个分区, 全局索引表有5个分区
create table t10 (c1 int, c2 int, c3 int, c4 int, primary key (c1)) partition by hash(c1) partitions 4;
inc $DDL_CNT;
create unique index g_unique_gkey_t10 on t10(c2,c3,c4) global partition by hash(c2) partitions 5;
inc $DDL_CNT;

############## 五创建一个表有多个索引情况，包括多个本地索引、本地唯一索引、全局索引、全局唯一索引
create table t11 (c1 int, c2 int, c3 int, c4 int, primary key (c1)) partition by hash(c1) partitions 4;
inc $DDL_CNT;
create unique index g_unique_gkey1_t11 on t11(c2,c3,c4) global partition by hash(c2) partitions 5;
inc $DDL_CNT;
create index g_normal_key_t11 on t11(c2,c3,c4) global partition by hash(c2) partitions 5;
inc $DDL_CNT;
create unique index g_unique_gkey2_t11 on t11(c2,c3,c4) global partition by hash(c2) partitions 5;
inc $DDL_CNT;
create unique index local_index_c1 on t11 (c1) local;
inc $DDL_CNT;
create unique index local_index_c2 on t11 (c1) local;
inc $DDL_CNT;
create unique index local_unique_index_c1 on t11 (c1) local;
inc $DDL_CNT;
create unique index local_unique_index_c2 on t11 (c1) local;
inc $DDL_CNT;

let $idx = 1;
while ($idx <= $DDL_CNT)
{
  ob1.oblog.next;
  inc $idx;
}


# DML
insert into t1 values(1, 2, 3, 4);
inc $DML_CNT;

begin;
insert into t2 values(1, 2, 3, 4);
update t1 set c2=20 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t3 values(1, 2, 3, 4);
update t1 set c2=21 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t4 values(1, 2, 3, 4);
update t1 set c2=22 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t5 values(1, 2, 3, 4);
update t1 set c2=23 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t6 values(1, 2, 3, 4);
update t1 set c2=24 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t7 values(1, 2, 3, 4);
update t1 set c2=25 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t8 values(1, 2, 3, 4);
update t1 set c2=26 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t9 values(1, 2, 3, 4);
update t1 set c2=27 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t10 values(1, 2, 3, 4);
update t1 set c2=28 where c1=1;
commit;
inc $DML_CNT;

begin;
insert into t11 values(1, 2, 3, 4);
update t1 set c2=29 where c1=1;
commit;
inc $DML_CNT;

let $idx = 1;
while ($idx <= $DML_CNT)
{
  ob1.oblog.next;
  inc $idx;
}
