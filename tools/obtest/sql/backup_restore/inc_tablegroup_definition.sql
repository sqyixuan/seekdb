# case 1: 基本测试
# 非分区tablegroup
create tablegroup inc_tg;
create table inc_tg_table_1(c1 int primary key, c2 int);
create table inc_tg_table_2(c1 int primary key, c2 int);

# hash
create tablegroup inc_tg_hash;
create table inc_table_with_tg_hash_1 (c1 int primary key, c2 int) partition by hash(c1) partitions 2;
create table inc_table_with_tg_hash_2 (c1 int primary key, c2 int) partition by hash(c1) partitions 2;
alter tablegroup inc_tg_hash add table inc_table_with_tg_hash_1, inc_table_with_tg_hash_2;
# hash-range
create tablegroup inc_tg_hash_range;
create table inc_table_with_tg_hash_range_1 (c1 int primary key, c2 int) partition by hash(c1) subpartition by range(c1) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990), SUBPARTITION c1 VALUES LESS THAN (2000), SUBPARTITION c2 VALUES LESS THAN MAXVALUE) partitions 2;
create table inc_table_with_tg_hash_range_2 (c1 int primary key, c2 int) partition by hash(c1) subpartition by range(c1) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990), SUBPARTITION c1 VALUES LESS THAN (2000), SUBPARTITION c2 VALUES LESS THAN MAXVALUE) partitions 2;
alter tablegroup inc_tg_hash_range add table inc_table_with_tg_hash_range_1, inc_table_with_tg_hash_range_2;
# hash-range_columns
create tablegroup inc_tg_hash_range_columns;
create table inc_table_with_tg_hash_range_columns_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by hash(c1) subpartition by range columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990, '1990'), SUBPARTITION c1 VALUES LESS THAN (2000, '2000'), SUBPARTITION c2 VALUES LESS THAN (MAXVALUE, MAXVALUE)) partitions 2;
create table inc_table_with_tg_hash_range_columns_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by hash(c1) subpartition by range columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990, '1990'), SUBPARTITION c1 VALUES LESS THAN (2000, '2000'), SUBPARTITION c2 VALUES LESS THAN (MAXVALUE, MAXVALUE)) partitions 2;
alter tablegroup inc_tg_hash_range_columns add table inc_table_with_tg_hash_range_columns_1, inc_table_with_tg_hash_range_columns_2;
# hash-list
create tablegroup inc_tg_hash_list;
create table inc_table_with_tg_hash_list_1 (c1 int primary key, c2 int) partition by hash(c1) subpartition by list(c1) subpartition template(SUBPARTITION c0 VALUES IN (1,2), SUBPARTITION c1 VALUES IN (3,4), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
create table inc_table_with_tg_hash_list_2 (c1 int primary key, c2 int) partition by hash(c1) subpartition by list(c1) subpartition template(SUBPARTITION c0 VALUES IN (1,2), SUBPARTITION c1 VALUES IN (3,4), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
alter tablegroup inc_tg_hash_list add table inc_table_with_tg_hash_list_1, inc_table_with_tg_hash_list_2;
# hash-list_columns
create tablegroup inc_tg_hash_list_columns;
create table inc_table_with_tg_hash_list_columns_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by hash(c1) subpartition by list columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES IN ((1,'1'),(2,'2')), SUBPARTITION c1 VALUES IN ((3,'3'),(4,'4')), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
create table inc_table_with_tg_hash_list_columns_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by hash(c1) subpartition by list columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES IN ((1,'1'),(2,'2')), SUBPARTITION c1 VALUES IN ((3,'3'),(4,'4')), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
alter tablegroup inc_tg_hash_list_columns add table inc_table_with_tg_hash_list_columns_1, inc_table_with_tg_hash_list_columns_2;

# key
create tablegroup inc_tg_key;
create table inc_table_with_tg_key_1 (c1 int primary key, c2 int) partition by key(c1) partitions 2;
create table inc_table_with_tg_key_2 (c1 int primary key, c2 int) partition by key(c1) partitions 2;
alter tablegroup inc_tg_key add table inc_table_with_tg_key_1, inc_table_with_tg_key_2;
# key-range
create tablegroup inc_tg_key_range;
create table inc_table_with_tg_key_range_1 (c1 int primary key, c2 int) partition by key(c1) subpartition by range(c1) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990), SUBPARTITION c1 VALUES LESS THAN (2000), SUBPARTITION c2 VALUES LESS THAN MAXVALUE) partitions 2;
create table inc_table_with_tg_key_range_2 (c1 int primary key, c2 int) partition by key(c1) subpartition by range(c1) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990), SUBPARTITION c1 VALUES LESS THAN (2000), SUBPARTITION c2 VALUES LESS THAN MAXVALUE) partitions 2;
alter tablegroup inc_tg_key_range add table inc_table_with_tg_key_range_1, inc_table_with_tg_key_range_2;
# key-range_columns
create tablegroup inc_tg_key_range_columns;
create table inc_table_with_tg_key_range_columns_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by key(c1) subpartition by range columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990, '1990'), SUBPARTITION c1 VALUES LESS THAN (2000, '2000'), SUBPARTITION c2 VALUES LESS THAN (MAXVALUE, MAXVALUE)) partitions 2;
create table inc_table_with_tg_key_range_columns_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by key(c1) subpartition by range columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES LESS THAN(1990, '1990'), SUBPARTITION c1 VALUES LESS THAN (2000, '2000'), SUBPARTITION c2 VALUES LESS THAN (MAXVALUE, MAXVALUE)) partitions 2;
alter tablegroup inc_tg_key_range_columns add table inc_table_with_tg_key_range_columns_1, inc_table_with_tg_key_range_columns_2;
# key-list
create tablegroup inc_tg_key_list;
create table inc_table_with_tg_key_list_1 (c1 int primary key, c2 int) partition by key(c1) subpartition by list(c1) subpartition template(SUBPARTITION c0 VALUES IN (1,2), SUBPARTITION c1 VALUES IN (3,4), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
create table inc_table_with_tg_key_list_2 (c1 int primary key, c2 int) partition by key(c1) subpartition by list(c1) subpartition template(SUBPARTITION c0 VALUES IN (1,2), SUBPARTITION c1 VALUES IN (3,4), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
alter tablegroup inc_tg_key_list add table inc_table_with_tg_key_list_1, inc_table_with_tg_key_list_2;
# key-list_columns
create tablegroup inc_tg_key_list_columns;
create table inc_table_with_tg_key_list_columns_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by key(c1) subpartition by list columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES IN ((1,'1'),(2,'2')), SUBPARTITION c1 VALUES IN ((3,'3'),(4,'4')), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
create table inc_table_with_tg_key_list_columns_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by key(c1) subpartition by list columns(c1, c2) subpartition template(SUBPARTITION c0 VALUES IN ((1,'1'),(2,'2')), SUBPARTITION c1 VALUES IN ((3,'3'),(4,'4')), SUBPARTITION c2 VALUES IN (DEFAULT)) partitions 2;
alter tablegroup inc_tg_key_list_columns add table inc_table_with_tg_key_list_columns_1, inc_table_with_tg_key_list_columns_2;

# list
create tablegroup inc_tg_list;
create table inc_table_with_tg_list_1 (c1 int primary key, c2 int) partition by list(c1) (PARTITION c0 VALUES IN (1,2), PARTITION c1 VALUES IN (3,4), PARTITION c2 VALUES IN (DEFAULT));
create table inc_table_with_tg_list_2 (c1 int primary key, c2 int) partition by list(c1) (PARTITION c0 VALUES IN (1,2), PARTITION c1 VALUES IN (3,4), PARTITION c2 VALUES IN (DEFAULT));
alter tablegroup inc_tg_list add table inc_table_with_tg_list_1, inc_table_with_tg_list_2;
# list-hash
create tablegroup inc_tg_list_hash;
create table inc_table_with_tg_list_hash_1 (c1 int primary key, c2 int) partition by list(c1) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES IN (1,2), PARTITION c1 VALUES IN (3,4), PARTITION c2 VALUES IN (DEFAULT));
create table inc_table_with_tg_list_hash_2 (c1 int primary key, c2 int) partition by list(c1) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES IN (1,2), PARTITION c1 VALUES IN (3,4), PARTITION c2 VALUES IN (DEFAULT));
alter tablegroup inc_tg_list_hash add table inc_table_with_tg_list_hash_1, inc_table_with_tg_list_hash_2;
# list-key
create tablegroup inc_tg_list_key;
create table inc_table_with_tg_list_key_1 (c1 int primary key, c2 int) partition by list(c1) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES IN (1,2), PARTITION c1 VALUES IN (3,4), PARTITION c2 VALUES IN (DEFAULT));
create table inc_table_with_tg_list_key_2 (c1 int primary key, c2 int) partition by list(c1) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES IN (1,2), PARTITION c1 VALUES IN (3,4), PARTITION c2 VALUES IN (DEFAULT));
alter tablegroup inc_tg_list_key add table inc_table_with_tg_list_key_1, inc_table_with_tg_list_key_2;

# list_columns
create tablegroup inc_tg_list_columns;
create table inc_table_with_tg_list_columns_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by list columns(c1, c2) (PARTITION c0 VALUES IN ((1,'1'),(2,'2')), PARTITION c1 VALUES IN ((3,'3'),(4,'4')), PARTITION c2 VALUES IN (DEFAULT));
create table inc_table_with_tg_list_columns_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by list columns(c1, c2) (PARTITION c0 VALUES IN ((1,'1'),(2,'2')), PARTITION c1 VALUES IN ((3,'3'),(4,'4')), PARTITION c2 VALUES IN (DEFAULT));
alter tablegroup inc_tg_list_columns add table inc_table_with_tg_list_columns_1, inc_table_with_tg_list_columns_2;
# list_columns-hash
create tablegroup inc_tg_list_columns_hash;
create table inc_table_with_tg_list_columns_hash_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by list columns(c1, c2) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES IN ((1,'1'),(2,'2')), PARTITION c1 VALUES IN ((3,'3'),(4,'4')), PARTITION c2 VALUES IN (DEFAULT));
create table inc_table_with_tg_list_columns_hash_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by list columns(c1, c2) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES IN ((1,'1'),(2,'2')), PARTITION c1 VALUES IN ((3,'3'),(4,'4')), PARTITION c2 VALUES IN (DEFAULT));
alter tablegroup inc_tg_list_columns_hash add table inc_table_with_tg_list_columns_hash_1, inc_table_with_tg_list_columns_hash_2;
# list_columns-key
create tablegroup inc_tg_list_columns_key;
create table inc_table_with_tg_list_columns_key_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by list columns(c1, c2) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES IN ((1,'1'),(2,'2')), PARTITION c1 VALUES IN ((3,'3'),(4,'4')), PARTITION c2 VALUES IN (DEFAULT));
create table inc_table_with_tg_list_columns_key_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by list columns(c1, c2) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES IN ((1,'1'),(2,'2')), PARTITION c1 VALUES IN ((3,'3'),(4,'4')), PARTITION c2 VALUES IN (DEFAULT));
alter tablegroup inc_tg_list_columns_key add table inc_table_with_tg_list_columns_key_1, inc_table_with_tg_list_columns_key_2;

# TODO:list-range

# case2: tablegroup级别增删分区

# 一级range
create tablegroup inc_tg_add_partition_range;
create table inc_table_add_partition_range_1 (c1 int primary key, c2 int) partition by range(c1) (PARTITION c0 VALUES LESS THAN (1980), PARTITION c1 VALUES LESS THAN (1990), PARTITION c2 VALUES LESS THAN (1995), PARTITION c3 VALUES LESS THAN (2000), PARTITION c4 VALUES LESS THAN MAXVALUE);
create table inc_table_add_partition_range_2 (c1 int primary key, c2 int) partition by range(c1) (PARTITION c0 VALUES LESS THAN (1980), PARTITION c1 VALUES LESS THAN (1990), PARTITION c2 VALUES LESS THAN (2000), PARTITION c4 VALUES LESS THAN MAXVALUE);
alter table inc_table_add_partition_range_1 drop partition (c0, c2);
alter table inc_table_add_partition_range_2 drop partition (c0);
alter tablegroup inc_tg_add_partition_range add table inc_table_add_partition_range_1, inc_table_add_partition_range_2;
alter tablegroup inc_tg_add_partition_range drop partition (c2);
#FIXME
alter tablegroup inc_tg_add_partition_range add partition (PARTITION c5 VALUES LESS THAN MAXVALUE);
#alter tablegroup inc_tg_add_partition_range add partition (PARTITION c5 VALUES LESS THAN (3000));

# 一级range_columns
create tablegroup inc_tg_add_partition_range_columns;
create table inc_table_add_partition_range_columns_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by range columns(c1, c2) (PARTITION c0 VALUES LESS THAN (1980, '1980'), PARTITION c1 VALUES LESS THAN (1990, '1990'), PARTITION c2 VALUES LESS THAN (1995, '1995'), PARTITION c3 VALUES LESS THAN (2000, '2000'), PARTITION c4 VALUES LESS THAN (MAXVALUE, MAXVALUE));
create table inc_table_add_partition_range_columns_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by range columns(c1, c2) (PARTITION c0 VALUES LESS THAN (1980, '1980'), PARTITION c1 VALUES LESS THAN (1990, '1990'), PARTITION c2 VALUES LESS THAN (2000, '2000'), PARTITION c4 VALUES LESS THAN (MAXVALUE, MAXVALUE));
alter table inc_table_add_partition_range_columns_1 drop partition (c0, c2);
alter table inc_table_add_partition_range_columns_2 drop partition (c0);
alter tablegroup inc_tg_add_partition_range_columns add table inc_table_add_partition_range_columns_1, inc_table_add_partition_range_columns_2;
alter tablegroup inc_tg_add_partition_range_columns drop partition (c2);
#FIXME
alter tablegroup inc_tg_add_partition_range_columns add partition (PARTITION c5 VALUES LESS THAN (MAXVALUE, MAXVALUE));
#alter tablegroup inc_tg_add_partition_range_columns add partition (PARTITION c5 VALUES LESS THAN (3000, '3000'));

# range-key
create tablegroup inc_tg_add_partition_range_key;
create table inc_table_add_partition_range_key_1 (c1 int primary key, c2 int) partition by range(c1) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980), PARTITION c1 VALUES LESS THAN (1990), PARTITION c2 VALUES LESS THAN (1995), PARTITION c3 VALUES LESS THAN (2000), PARTITION c4 VALUES LESS THAN MAXVALUE);
create table inc_table_add_partition_range_key_2 (c1 int primary key, c2 int) partition by range(c1) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980), PARTITION c1 VALUES LESS THAN (1990), PARTITION c2 VALUES LESS THAN (2000), PARTITION c4 VALUES LESS THAN MAXVALUE);
alter table inc_table_add_partition_range_key_1 drop partition (c0, c2);
alter table inc_table_add_partition_range_key_2 drop partition (c0);
alter tablegroup inc_tg_add_partition_range_key add table inc_table_add_partition_range_key_1, inc_table_add_partition_range_key_2;
alter tablegroup inc_tg_add_partition_range_key drop partition (c2);
#FIXME
alter tablegroup inc_tg_add_partition_range_key add partition (PARTITION c5 VALUES LESS THAN MAXVALUE);
#alter tablegroup inc_tg_add_partition_range_key add partition (PARTITION c5 VALUES LESS THAN (3000));

# range-hash
create tablegroup inc_tg_add_partition_range_hash;
create table inc_table_add_partition_range_hash_1 (c1 int primary key, c2 int) partition by range(c1) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980), PARTITION c1 VALUES LESS THAN (1990), PARTITION c2 VALUES LESS THAN (1995), PARTITION c3 VALUES LESS THAN (2000), PARTITION c4 VALUES LESS THAN MAXVALUE);
create table inc_table_add_partition_range_hash_2 (c1 int primary key, c2 int) partition by range(c1) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980), PARTITION c1 VALUES LESS THAN (1990), PARTITION c2 VALUES LESS THAN (2000), PARTITION c4 VALUES LESS THAN MAXVALUE);
alter table inc_table_add_partition_range_hash_1 drop partition (c0, c2);
alter table inc_table_add_partition_range_hash_2 drop partition (c0);
alter tablegroup inc_tg_add_partition_range_hash add table inc_table_add_partition_range_hash_1, inc_table_add_partition_range_hash_2;
alter tablegroup inc_tg_add_partition_range_hash drop partition (c2);
#FIXME
alter tablegroup inc_tg_add_partition_range_hash add partition (PARTITION c5 VALUES LESS THAN MAXVALUE);
#alter tablegroup inc_tg_add_partition_range_hash add partition (PARTITION c5 VALUES LESS THAN (3000));

# range_columns-key
create tablegroup inc_tg_add_partition_range_columns_key;
create table inc_table_add_partition_range_columns_key_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by range columns(c1, c2) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980, '1980'), PARTITION c1 VALUES LESS THAN (1990, '1990'), PARTITION c2 VALUES LESS THAN (1995, '1995'), PARTITION c3 VALUES LESS THAN (2000, '2000'), PARTITION c4 VALUES LESS THAN (MAXVALUE, MAXVALUE));
create table inc_table_add_partition_range_columns_key_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by range columns(c1, c2) subpartition by key(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980, '1980'), PARTITION c1 VALUES LESS THAN (1990, '1990'), PARTITION c2 VALUES LESS THAN (2000, '2000'), PARTITION c4 VALUES LESS THAN (MAXVALUE, MAXVALUE));
alter table inc_table_add_partition_range_columns_key_1 drop partition (c0, c2);
alter table inc_table_add_partition_range_columns_key_2 drop partition (c0);
alter tablegroup inc_tg_add_partition_range_columns_key add table inc_table_add_partition_range_columns_key_1, inc_table_add_partition_range_columns_key_2;
alter tablegroup inc_tg_add_partition_range_columns_key drop partition (c2);
#FIXME
alter tablegroup inc_tg_add_partition_range_columns_key add partition (PARTITION c5 VALUES LESS THAN (MAXVALUE, MAXVALUE));
#alter tablegroup inc_tg_add_partition_range_columns_key add partition (PARTITION c5 VALUES LESS THAN (3000, '3000'));

# range_columns-hash
create tablegroup inc_tg_add_partition_range_columns_hash;
create table inc_table_add_partition_range_columns_hash_1 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by range columns(c1, c2) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980, '1980'), PARTITION c1 VALUES LESS THAN (1990, '1990'), PARTITION c2 VALUES LESS THAN (1995, '1995'), PARTITION c3 VALUES LESS THAN (2000, '2000'), PARTITION c4 VALUES LESS THAN (MAXVALUE, MAXVALUE));
create table inc_table_add_partition_range_columns_hash_2 (c1 int, c2 varchar(20), primary key(c1, c2)) partition by range columns(c1, c2) subpartition by hash(c1) subpartitions 2 (PARTITION c0 VALUES LESS THAN (1980, '1980'), PARTITION c1 VALUES LESS THAN (1990, '1990'), PARTITION c2 VALUES LESS THAN (2000, '2000'), PARTITION c4 VALUES LESS THAN (MAXVALUE, MAXVALUE));
alter table inc_table_add_partition_range_columns_hash_1 drop partition (c0, c2);
alter table inc_table_add_partition_range_columns_hash_2 drop partition (c0);
alter tablegroup inc_tg_add_partition_range_columns_hash add table inc_table_add_partition_range_columns_hash_1, inc_table_add_partition_range_columns_hash_2;
alter tablegroup inc_tg_add_partition_range_columns_hash drop partition (c2);
#FIXME
alter tablegroup inc_tg_add_partition_range_columns_hash add partition (PARTITION c5 VALUES LESS THAN (MAXVALUE, MAXVALUE));
#alter tablegroup inc_tg_add_partition_range_columns_hash add partition (PARTITION c5 VALUES LESS THAN (3000, '3000'));

# TODO: range-list


## case 3: 非分区表分裂
## hash
#create tablegroup inc_tg_partitioned_hash;
#create table inc_tg_partitioned_hash_t1 (c1 int primary key, c2 int);
#create table inc_tg_partitioned_hash_t2 (c3 int primary key, c4 int);
#alter table inc_tg_partitioned_hash_t1 modify partition by hash(c1) partitions 1;
#alter table inc_tg_partitioned_hash_t2 modify partition by hash(c2) partitions 1;
#alter tablegroup inc_tg_partitioned_hash modify partition by hash partitions 2;
## key
#create tablegroup inc_tg_partitioned_key;
#create table inc_tg_partitioned_key_t1 (c1 int primary key, c2 int);
#create table inc_tg_partitioned_key_t2 (c3 int primary key, c4 int);
#alter table inc_tg_partitioned_key_t1 modify partition by key(c1) partitions 1;
#alter table inc_tg_partitioned_key_t2 modify partition by key(c2) partitions 1;
#alter tablegroup inc_tg_partitioned_key modify partition by key 1 partitions 2;
## range
#create tablegroup inc_tg_partitioned_range;
#create table inc_tg_partitioned_range_t1 (c1 int primary key, c2 int);
#create table inc_tg_partitioned_range_t2 (c3 int primary key, c4 int);
#alter table inc_tg_partitioned_range_t1 modify partition by range(c1) (partition p0 values less than (MAXVALUE));
#alter table inc_tg_partitioned_range_t2 modify partition by range(c3) (partition p0 values less than (MAXVALUE));
#alter tablegroup inc_tg_partitioned_range modify partition by range (partition p0 values less than (100), partition p1 values less than (MAXVALUE));
## range columns
#create tablegroup inc_tg_partitioned_range_columns;
#create table inc_tg_partitioned_range_columns_t1 (c1 int, c2 varchar(20), primary key(c1, c2));
#create table inc_tg_partitioned_range_columns_t2 (c3 int, c4 varchar(20), primary key(c1, c2));
#alter table inc_tg_partitioned_range_columns_t1 modify partition by range_columns(c1, c2) (partition p0 values less than (MAXVALUE, MAXVALUE));
#alter table inc_tg_partitioned_range_columns_t2 modify partition by range_columns(c3, c4) (partition p0 values less than (MAXVALUE, MAXVALUE));
#alter tablegroup inc_tg_partitioned_range_columns modify partition by range_columns (partition p0 values less than (100, '100'), partition p1 values less than (MAXVALUE, MAXVALUE));
## TODO: 后续分裂支持list分区or二级分区，补充相应的case
#
## 实际上目前hash/key分区的part_id只可能从0 or 1开始
#create table inc_hash_with_max_used_part_id (c1 int primary key, c2 int) max_used_part_id = 10 partition by hash(c1) partitions 5;
#create table inc_key_with_max_used_part_id (c1 int primary key, c2 int) max_used_part_id = 10 partition by key(c1) partitions 5;

# case 4: 2.0之前创建的tablegroup
create user if not exists __oceanbase_inner_drc_user;
grant all privileges on *.* to __oceanbase_inner_drc_user;
sleep 10;
connect (conn_drc,$sourceIp,__oceanbase_inner_drc_user@$tntName,,oceanbase,$sourcePort);
eval create tablegroup inc_tg_with_tablegroup_id_1 tablegroup_id = 100;

connection source_data_conn;
#drop user __oceanbase_inner_drc_user;
