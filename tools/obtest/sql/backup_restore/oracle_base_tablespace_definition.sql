create tablespace backup_test_tablespace1;
create tablespace backup_test_tablespace2;
create table test_table_for_back_tablesapce(v1 int) tablespace backup_test_tablespace1;
create table test_table_for_back_tablesapce2(v1 int) tablespace backup_test_tablespace2;

create table test_tp_list_partition(v1 int , v2 int) partition by list(v1) 
(partition p1 values (1) tablespace backup_test_tablespace1,
 partition p2 values (2) tablespace backup_test_tablespace1
);

create table test_tp_range_partition(v1 int , v2 int) partition by range(v1)
(partition p1 values less than (1) tablespace backup_test_tablespace1,
 partition p2 values less than (2) tablespace backup_test_tablespace1
);

create table test_tp_hash_partition(v1 int , v2 int) partition by hash(v1) partitions 4 tablespace  backup_test_tablespace1;



