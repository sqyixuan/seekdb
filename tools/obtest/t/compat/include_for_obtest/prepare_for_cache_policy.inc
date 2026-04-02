use test;
create table cp_test_table_t3(col1 int, col2 int)
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

create index idx2 on cp_test_table_t3 (col2);