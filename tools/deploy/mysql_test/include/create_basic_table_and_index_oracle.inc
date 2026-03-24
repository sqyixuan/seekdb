# 限制:
# 1. sys租户需要开GTS才能支持全局索引
# 2. 全局索引不支持二级分区

# case 1: sys
connection sys;
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

# case 3: oracle
connection oracle;
# case 3.1: non-partitioned
eval create table non_partitioned_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5));
eval create index local_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2) local;
eval create index global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c3) global;
eval create index hash_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c3) global partition by hash(c2, c3) partitions 2;
eval create index range_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c4) global partition by range(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2, c5) global partition by list(c2, c5) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));

# case 3.2: hash
eval create table hash_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by hash(c1, c2) partitions 2;
eval create index local_idx_for_hash_tb_$postfix on hash_tb_$postfix(c2) local;
eval create index global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c3) global;
eval create index hash_global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c2, c3) global partition by hash(c2, c3) partitions 2;
eval create index range_global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c2, c4) global partition by range(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c2, c5) global partition by list(c2, c5) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));

# case 3.3: range
eval create table range_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by range(c1, c2) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index local_idx_for_range_tb_$postfix on range_tb_$postfix(c2) local;
eval create index global_idx_for_range_tb_$postfix on range_tb_$postfix(c3) global;
eval create index hash_global_idx_for_range_tb_$postfix on range_tb_$postfix(c2, c3) global partition by hash(c2, c3) partitions 2;
eval create index range_global_idx_for_range_tb_$postfix on range_tb_$postfix(c2, c4) global partition by range(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_global_idx_for_range_tb_$postfix on range_tb_$postfix(c2, c5) global partition by list(c2, c5) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));

# case 3.4: list
eval create table list_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by list(c1, c2) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));
eval create index local_idx_for_list_tb_$postfix on list_tb_$postfix(c2) local;
eval create index global_idx_for_list_tb_$postfix on list_tb_$postfix(c3) global;
eval create index hash_global_idx_for_list_tb_$postfix on list_tb_$postfix(c2, c3) global partition by hash(c2, c3) partitions 2;
eval create index range_global_idx_for_list_tb_$postfix on list_tb_$postfix(c2, c4) global partition by range(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_global_idx_for_list_tb_$postfix on list_tb_$postfix(c2, c5) global partition by list(c2, c5) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));

# case 3.5: hash - list
eval create table hash_list_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by hash(c1, c2) subpartition by list(c1, c2) subpartition template (subpartition p0 values ((100, 100)), subpartition p1 values ((200, 200))) partitions 2;
eval create index local_idx_for_hash_list_tb_$postfix on hash_list_tb_$postfix(c2) local;
eval create index global_idx_for_hash_list_tb_$postfix on hash_list_tb_$postfix(c3) global;
eval create index hash_global_idx_for_hash_list_tb_$postfix on hash_list_tb_$postfix(c2, c3) global partition by hash(c2, c3) partitions 2;
eval create index range_global_idx_for_hash_list_tb_$postfix on hash_list_tb_$postfix(c2, c4) global partition by range(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_global_idx_for_hash_list_tb_$postfix on hash_list_tb_$postfix(c2, c5) global partition by list(c2, c5) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));

# case 3.6: range - hash
eval create table range_hash_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by range(c1, c2) subpartition by hash(c3, c4) subpartitions 2 (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index local_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c2) local;
eval create index global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c3) global;
eval create index hash_global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c2, c3) global partition by hash(c2, c3) partitions 2;
eval create index range_global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c2, c4) global partition by range(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c2, c5) global partition by list(c2, c5) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));

# case 3.7: list - range
eval create table list_range_tb_$postfix (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1, c2, c3, c4, c5)) partition by list(c1, c2) subpartition by range(c3, c4) subpartition template (subpartition p0 values less than (100, 100), subpartition p1 values less than (200, 200)) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));
eval create index local_idx_for_list_range_tb_$postfix on list_range_tb_$postfix(c2) local;
eval create index global_idx_for_list_range_tb_$postfix on list_range_tb_$postfix(c3) global;
eval create index hash_global_idx_for_list_range_tb_$postfix on list_range_tb_$postfix(c2, c3) global partition by hash(c2, c3) partitions 2;
eval create index range_global_idx_for_list_range_tb_$postfix on list_range_tb_$postfix(c2, c4) global partition by range(c2, c4) (partition p0 values less than (100, 100), partition p1 values less than (200, 200));
eval create index list_global_idx_for_list_range_tb_$postfix on list_range_tb_$postfix(c2, c5) global partition by list(c2, c5) (partition p0 values ((100, 100)), partition p1 values ((200, 200)));
