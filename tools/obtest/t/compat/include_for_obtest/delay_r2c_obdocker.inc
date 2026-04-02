connection tentsql_conn;
--disable_all_result_log
use testsql;
#创建行存/行列混存表
create table single_cg_with_multi_columns (c1 int primary key, c2 int, c3 int) with column group (all columns, each column);
create table all_cg_not_overwrite_0 (c1 int primary key, c2 int, c3 int) with column group (all columns);
create table all_cg_not_overwrite_1 (c1 int primary key, c2 int, c3 int) with column group (all columns, each column);
#插入数据
insert into single_cg_with_multi_columns values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into all_cg_not_overwrite_0 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into all_cg_not_overwrite_1 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
#变换表的存储类型
#变为行存表
alter table single_cg_with_multi_columns drop column group (all columns, each column);
alter table single_cg_with_multi_columns add column group (all columns);
#变为行存表
alter table all_cg_not_overwrite_0 drop column group (all columns);
alter table all_cg_not_overwrite_0 add column group (all columns);
#变为行存表
alter table all_cg_not_overwrite_1 drop column group (each column);
alter table all_cg_not_overwrite_1 add column group (each column);
alter table all_cg_not_overwrite_1 drop column group (all columns);
alter table all_cg_not_overwrite_1 add column group (all columns);
alter table all_cg_not_overwrite_1 drop column group (each column);


#使所有的表最后都变成列存表
#分别创建行存/列存/行列混存表
create table c2r_1 (c1 int primary key, c2 int, c3 int) with column group (all columns);
create table c2r_2 (c1 int primary key, c2 int, c3 int) with column group (each column);
create table c2r_3 (c1 int primary key, c2 int, c3 int) with column group (all columns, each column);
insert into c2r_1 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into c2r_2 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into c2r_3 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
#升级前均转换成列存表
alter table c2r_1 drop column group (all columns);
alter table c2r_1 add column group (each column);

alter table c2r_2 ADD COLUMN GROUP(all columns);
alter table c2r_2 drop column group(all columns);

alter table c2r_3 drop column group (all columns);
alter table c2r_3 drop column group (each column);
alter table c2r_3 add column group (all columns);
alter table c2r_3 drop column group (all columns);
alter table c2r_3 add column group (each column);

#使所有的表最后都变成行列混存表,升级后转行存
create table cr2r_1 (c1 int primary key, c2 int, c3 int) with column group (all columns);
create table cr2r_2 (c1 int primary key, c2 int, c3 int) with column group (each column);
create table cr2r_3 (c1 int primary key, c2 int, c3 int) with column group (all columns, each column);
insert into cr2r_1 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into cr2r_2 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into cr2r_3 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
#升级前均转化为行列混存表
alter table cr2r_1 drop column group (all columns);
alter table cr2r_1 add column group (each column);
alter table cr2r_1 add column group(all columns);

alter table cr2r_2 drop column group (each column);
alter table cr2r_2 add column group (all columns);
alter table cr2r_2 add column group(each column);

alter table cr2r_3 drop column group (all columns);
alter table cr2r_3 drop column group (each column);
alter table cr2r_3 add column group (all columns);
alter table cr2r_3 drop column group (all columns);
alter table cr2r_3 add column group (each column);
alter table cr2r_3 drop column group (each column);
alter table cr2r_3 add column group (all columns, each column);

#使所有的表最后都变成行列混存表,升级后转列存
create table cr2c_1 (c1 int primary key, c2 int, c3 int) with column group (all columns);
create table cr2c_2 (c1 int primary key, c2 int, c3 int) with column group (each column);
create table cr2c_3 (c1 int primary key, c2 int, c3 int) with column group (all columns, each column);
insert into cr2c_1 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into cr2c_2 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
insert into cr2c_3 values (1, 2, 3), (2, 3, 4), (3, 4, 5);
#升级前均转化为行列混存表
alter table cr2c_1 drop column group (all columns);
alter table cr2c_1 add column group (each column);
alter table cr2c_1 add column group(all columns);

alter table cr2c_2 drop column group (each column);
alter table cr2c_2 add column group (all columns);
alter table cr2c_2 add column group(each column);

alter table cr2c_3 drop column group (all columns);
alter table cr2c_3 drop column group (each column);
alter table cr2c_3 add column group (all columns);
alter table cr2c_3 drop column group (all columns);
alter table cr2c_3 add column group (each column);
alter table cr2c_3 drop column group (each column);
alter table cr2c_3 add column group (all columns, each column);
--enable_all_result_log
