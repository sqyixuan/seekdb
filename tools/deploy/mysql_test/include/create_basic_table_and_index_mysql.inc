# 限制:
# 1. sys租户需要开GTS才能支持全局索引
# 2. 全局索引不支持二级分区

# case 1: sys
connection sys;
--disable_warnings
drop database if exists test;
create database test;
--enable_warnings
use test;
# case 1.1: non-partitioned
eval create table non_partitioned_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5));
eval create index local_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2) local;
#eval create index global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c3) global;
#eval create index key_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
#eval create index range_columns_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#eval create index list_columns_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.2: key
eval create table key_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by key(c1, c2) partitions 2;
eval create index local_idx_for_key_tb_$postfix on key_tb_$postfix(c2) local;
#eval create index global_idx_for_key_tb_$postfix on key_tb_$postfix(c3) global;
#eval create index key_global_idx_for_key_tb_$postfix on key_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
#eval create index range_columns_global_idx_for_key_tb_$postfix on key_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#eval create index list_columns_global_idx_for_key_tb_$postfix on key_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.3: range columns
eval create table range_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by range columns(c1, c2) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index local_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2) local;
#eval create index global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c3) global;
#eval create index key_global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
#eval create index range_columns_global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#eval create index list_columns_global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.4: list columns
eval create table list_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by list columns(c1, c2) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));
eval create index local_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2) local;
#eval create index global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c3) global;
#eval create index key_global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
#eval create index range_columns_global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#eval create index list_columns_global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.5: key - list columns
eval create table key_list_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by key(c1, c2) subpartition by list columns(c1, c2) subpartition template (subpartition p0 values in ((100, 100)), subpartition p1 values in ((200, 200))) partitions 2;
eval create index local_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2) local;
#eval create index global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c3) global;
#eval create index key_global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
#eval create index range_columns_global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#eval create index list_columns_global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.6: range columns - key
eval create table range_columns_key_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by range columns(c1, c2) subpartition by key(c3, c4) subpartitions 2 (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index local_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2) local;
#eval create index global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c3) global;
#eval create index key_global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
#eval create index range_columns_global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#eval create index list_columns_global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.7: list columns - range columns
eval create table list_columns_range_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by list columns(c1, c2) subpartition by range columns(c3, c4) subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));
eval create index local_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2) local;
#eval create index global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c3) global;
#eval create index key_global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
#eval create index range_columns_global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
#eval create index list_columns_global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.8: spatial index
eval create table spatial_col_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 geometry not null srid 0, primary key(c1, c2, c3, c4)) partition by key(c1, c2) partitions 2;
eval create spatial index local_idx_for_spatial_tb_$postfix on spatial_col_tb_$postfix(c5) local;

# case 1.9: mysql compatible date types
eval create table mysql_date_tb_$postfix (c1 date);
eval create table mysql_datetime_tb_$postfix (c1 datetime);

# case 2: mysql
connection mysql;
--disable_warnings
drop database if exists test;
create database test;
--enable_warnings
use test;
# case 2.1: non-partitioned
eval create table non_partitioned_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5));
eval create index local_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2) local;
eval create index global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c3) global;
eval create index key_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
eval create index range_columns_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_columns_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 2.2: key
eval create table key_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by key(c1, c2) partitions 2;
eval create index local_idx_for_key_tb_$postfix on key_tb_$postfix(c2) local;
eval create index global_idx_for_key_tb_$postfix on key_tb_$postfix(c3) global;
eval create index key_global_idx_for_key_tb_$postfix on key_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
eval create index range_columns_global_idx_for_key_tb_$postfix on key_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_columns_global_idx_for_key_tb_$postfix on key_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 2.3: range columns
eval create table range_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by range columns(c1, c2) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index local_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2) local;
eval create index global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c3) global;
eval create index key_global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
eval create index range_columns_global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_columns_global_idx_for_range_columns_tb_$postfix on range_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 2.4: list columns
eval create table list_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by list columns(c1, c2) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));
eval create index local_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2) local;
eval create index global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c3) global;
eval create index key_global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
eval create index range_columns_global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_columns_global_idx_for_list_columns_tb_$postfix on list_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 2.5: key - list columns
eval create table key_list_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by key(c1, c2) subpartition by list columns(c1, c2) subpartition template (subpartition p0 values in ((100, 100)), subpartition p1 values in ((200, 200))) partitions 2;
eval create index local_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2) local;
eval create index global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c3) global;
eval create index key_global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
eval create index range_columns_global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_columns_global_idx_for_key_list_columns_tb_$postfix on key_list_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 2.6: range columns - key
eval create table range_columns_key_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by range columns(c1, c2) subpartition by key(c3, c4) subpartitions 2 (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index local_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2) local;
eval create index global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c3) global;
eval create index key_global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
eval create index range_columns_global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_columns_global_idx_for_range_columns_key_tb_$postfix on range_columns_key_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 2.7: list columns - range columns
eval create table list_columns_range_columns_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by list columns(c1, c2) subpartition by range columns(c3, c4) subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));
eval create index local_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2) local;
eval create index global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c3) global;
eval create index key_global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2, c3) global partition by key(c2, c3) partitions 2;
eval create index range_columns_global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2, c4) global partition by range columns(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_columns_global_idx_for_list_columns_range_columns_tb_$postfix on list_columns_range_columns_tb_$postfix(c2, c5) global partition by list columns(c2, c5) (partition p0 values in ((100, 100)), partition p1 values in ((200, 200)));

# case 1.8: spatial index
eval create table spatial_col_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 geometry not null srid 0, primary key(c1, c2, c3, c4)) partition by key(c1, c2) partitions 2;
eval create spatial index local_idx_for_spatial_tb_$postfix on spatial_col_tb_$postfix(c5) local;

# case 2.9: mysql compatible date types
eval create table mysql_date_tb_$postfix (c1 date);
eval create table mysql_datetime_tb_$postfix (c1 datetime);
