# 测试新增配置项
use test;
--disable_warnings
--error 0,4179
drop table if exists cp_test_table_t1;
--error 0,4179
drop table if exists cp_test_table_t2;
--enable_warnings

# check set storage cache policy for create table
--error 0,1235
create table cp_test_table_t1(c1 int, c2 char(10)) storage_cache_policy= (global = "hot");
--error 0,1235
create table cp_test_table_t2(col1 int, col2 int)
storage_cache_policy = (boundary_column = col1, boundary_column_unit = 'ms', hot_retention = 1 month)
partition by range(col1)
subpartition by list(col2)
(partition p0 values less than(100)
(subpartition sp0 values in (1, 2, 3),
subpartition sp1 values in (4, 5, 6)
),
partition p1 values less than(200)
(subpartition sp2 values in (1, 2, 3),
subpartition sp3 values in (4, 5, 6),
subpartition sp4 values in (7, 8, 9)
)
);

# check set storage cache policy for alter table
alter table cp_test_table_t3 alter partition p0 p1 storage_cache_policy='hot';

# check set index with storage cache policy
create index idx1 on cp_test_table_t3 (col1) local storage_cache_policy (global = "hot");
# check set for exist index
alter table cp_test_table_t3 alter index idx2 storage_cache_policy (global = "auto");

--disable_warnings
--error 0,4179
drop table if exists cp_test_table_t1;
--error 0,4179
drop table if exists cp_test_table_t2;
--enable_warnings