connection tentsql_conn;
use testsql;
--disable_all_result_log
insert into single_cg_with_multi_columns values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into all_cg_not_overwrite_0 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into all_cg_not_overwrite_1 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
select * from single_cg_with_multi_columns;
select * from all_cg_not_overwrite_0;
select * from all_cg_not_overwrite_1;

insert into c2r_1 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into c2r_2 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into c2r_3 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
select * from c2r_1;
select * from c2r_2;
select * from c2r_3;

insert into cr2r_1 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into cr2r_2 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into cr2r_3 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
select * from cr2r_1;
select * from cr2r_2;
select * from cr2r_3;

insert into cr2c_1 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into cr2c_2 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
insert into cr2c_3 values (7, 8, 9), (8, 9, 10), (9, 10, 11);
select * from cr2c_1;
select * from cr2c_2;
select * from cr2c_3;
--enable_all_result_log
#将以下行存表变为行列存混存表，且异步执行
alter table single_cg_with_multi_columns add column group (each column) delayed;
alter table all_cg_not_overwrite_0 add column group (each column) delayed;
alter table all_cg_not_overwrite_1 add column group (each column) delayed;

#将以下列存表转为行列混存存表
alter table c2r_1 add column group (all columns);
alter table c2r_2 add column group (all columns);
alter table c2r_3 add column group (all columns);

alter table c2r_1 drop column group (each column);
alter table c2r_2 drop column group (each column);
alter table c2r_3 drop column group (each column);

alter table c2r_1 drop column group (all columns);
alter table c2r_2 drop column group (all columns);
alter table c2r_3 drop column group (all columns);

alter table c2r_1 add column group (each column) delayed;
alter table c2r_2 add column group (each column) delayed;
alter table c2r_3 add column group (each column) delayed;

#将以下行列混存存表转为转行存
alter table cr2r_1 drop column group (each column);
alter table cr2r_2 drop column group (each column);
alter table cr2r_3 drop column group (each column);

alter table cr2r_1 drop column group (all columns);
alter table cr2r_2 drop column group (all columns);
alter table cr2r_3 drop column group (all columns);

alter table cr2r_1 add column group(all columns, each column) delayed;
alter table cr2r_2 add column group(all columns, each column) delayed;
alter table cr2r_3 add column group(all columns, each column) delayed;

#将以下行列混存存表转为转列存
alter table cr2c_1 drop column group (all columns);
alter table cr2c_2 drop column group (all columns);
alter table cr2c_3 drop column group (all columns);

alter table cr2c_1 drop column group (each column);
alter table cr2c_2 drop column group (each column);
alter table cr2c_3 drop column group (each column);

alter table cr2c_1 add column group(all columns, each column) delayed;
alter table cr2c_2 add column group(all columns, each column) delayed;
alter table cr2c_3 add column group(all columns, each column) delayed;
--disable_all_result_log
insert into single_cg_with_multi_columns values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into all_cg_not_overwrite_0 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into all_cg_not_overwrite_1 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
select * from single_cg_with_multi_columns;
select * from all_cg_not_overwrite_0;
select * from all_cg_not_overwrite_1;

insert into c2r_1 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into c2r_2 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into c2r_3 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
select * from c2r_1;
select * from c2r_2;
select * from c2r_3;

insert into cr2r_1 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into cr2r_2 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into cr2r_3 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
select * from cr2r_1;
select * from cr2r_2;
select * from cr2r_3;

insert into cr2c_1 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into cr2c_2 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
insert into cr2c_3 values (10, 11, 12), (11, 12, 13), (12, 13, 14);
select * from cr2c_1;
select * from cr2c_2;
select * from cr2c_3;
--enable_all_result_log
