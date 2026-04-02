# set EN_ENABLE_SHARED_STORAGE_COLUMN_GROUP to enable column group under shared-storage mode
ob1.mysql alter system set_tp tp_no = 2502, error_code = 4016, frequency = 1;
connection conn_mysql;
create table test01(c1 int, c2 int, c3 varchar(40960), c4 int) row_format = compact, compression= 'none';
create index idx01 on test01(c2, c4) partition by hash(c2) partitions 2;
create table test02(c0 varchar(4096), c1 int, c2 int, c3 int, c4 varchar(40960), c5 varchar(40960), c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, primary key(c0, c1)) with column group (all columns, each column);
create table test03(c0 varchar(4096), c1 int, c2 int, c3 int, c4 varchar(40960), c5 varchar(40960), c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, primary key(c0, c1)) with column group (each column);
create table test04(c1 int, c2 int, c3 varchar(40960), c4 int) compression= 'none';
create index idx04 on test04(c4, c2) partition by hash(c2) partitions 2;