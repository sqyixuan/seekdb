--disable_query_log
# 1. global index is not supported when gts is off

connection sys;
# table
use test;
eval create table non_partitioned_tb_$postfix (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3, c4));
eval create index local_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c1) local;
#eval create index global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2) global;
#eval create index hash_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c3) global partition by hash(c3) partitions 2;

eval create table hash_tb_$postfix (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3, c4)) partition by hash(c1) partitions 2;
eval create index local_idx_for_hash_tb_$postfix on hash_tb_$postfix(c1) local;
#eval create index global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c2) global;
#eval create index hash_global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c3) global partition by hash(c3) partitions 2;

eval create table range_hash_tb_$postfix (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3, c4)) partition by range(c1) subpartition by hash(c2) subpartitions 2 (partition p0 values less than(100), partition p1 values less than(200));
eval create index local_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c1) local;
#eval create index global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c2) global;
#eval create index hash_global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c3) global partition by hash(c3) partitions 2;

# view
use test;
eval create view user_view_$postfix as select * from test.non_partitioned_tb_$postfix;

# procedure、function
use test;
delimiter //;
eval create procedure procedure_$postfix(out in_name varchar(20))
begin
  declare i int default 0;
  select lower(e.job) into in_name  from t1 e where lower(e.ename) = 'james' order by e.empno;
  while i <= 10 do
  insert into t1 values(7596,lower('KoBe'),lower('Clerk'),7902,'1980-07-18',1000,1200,20);
  set i := i + 1;
  end while;
end;//

eval create function function_$postfix(v1 int) returns time(6) return v1;//
delimiter ;//

# trigger
use test;
eval create trigger tr_$postfix before insert on test.non_partitioned_tb_$postfix for each row set NEW.c2 = NEW.c1 * 2;

# database
eval create database db_$postfix;

# tablegroup
eval create tablegroup non_partitioned_tg_$postfix;
--disable_warnings
eval create tablegroup hash_tg_$postfix partition by hash partitions 2;
eval create tablegroup range_hash_tg_$postfix partition by range subpartition by hash subpartitions 2 (partition p0 values less than(100), partition p1 values less than(200));
--enable_warnings

#connection mysql;
# table
#use test;
#eval create table non_partitioned_tb_$postfix (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3, c4));
#eval create index local_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c1) local;
#eval create index global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c2) global;
#eval create index hash_global_idx_for_non_partitioned_tb_$postfix on non_partitioned_tb_$postfix(c3) global partition by hash(c3) partitions 2;
#
#eval create table hash_tb_$postfix (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3, c4)) partition by hash(c1) partitions 2;
#eval create index local_idx_for_hash_tb_$postfix on hash_tb_$postfix(c1) local;
#eval create index global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c2) global;
#eval create index hash_global_idx_for_hash_tb_$postfix on hash_tb_$postfix(c3) global partition by hash(c3) partitions 2;
#
#eval create table range_hash_tb_$postfix (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3, c4)) partition by range(c1) subpartition by hash(c2) subpartitions 2 (partition p0 values less than(100), partition p1 values less than(200));
#eval create index local_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c1) local;
#eval create index global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c2) global;
#eval create index hash_global_idx_for_range_hash_tb_$postfix on range_hash_tb_$postfix(c3) global partition by hash(c3) partitions 2;

# view
#use test;
#eval create view user_view_$postfix as select * from test.non_partitioned_tb_$postfix;

# trigger
#use test;
#eval create trigger tr_$postfix before insert on test.non_partitioned_tb_$postfix for each row set NEW.c2 = NEW.c1 * 2;

# procedure、function
#use test;
#delimiter //;
#eval create procedure procedure_$postfix(out in_name varchar(20))
#begin
#declare i int default 0;
  #select lower(e.job) into in_name  from t1 e where lower(e.ename) = 'james' order by e.empno;
  #while i <= 10 do
  #insert into t1 values(7596,lower('KoBe'),lower('Clerk'),7902,'1980-07-18',1000,1200,20);
  #  set i := i + 1;
#  end while;
#end;//

#eval create function function_$postfix(v1 int) returns time(6) return v1;//
#delimiter ;//

# database
#eval create database db_$postfix;

# tablegroup
#eval create tablegroup non_partitioned_tg_$postfix;
#--disable_warnings
#eval create tablegroup hash_tg_$postfix partition by hash partitions 2;
#eval create tablegroup range_hash_tg_$postfix partition by range subpartition by hash subpartitions 2 (partition p0 values less than(100), partition p1 values less than(200));
#--enable_warnings
--enable_query_log
