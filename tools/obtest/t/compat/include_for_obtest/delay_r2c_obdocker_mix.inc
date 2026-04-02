connection tentsql_conn;
use testsql;
--disable_all_result_log
insert into single_cg_with_multi_columns values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into all_cg_not_overwrite_0 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into all_cg_not_overwrite_1 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
select * from single_cg_with_multi_columns;
select * from all_cg_not_overwrite_0;
select * from all_cg_not_overwrite_1;

insert into c2r_1 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into c2r_2 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into c2r_3 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
select * from c2r_1;
select * from c2r_2;
select * from c2r_3;

insert into cr2r_1 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into cr2r_2 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into cr2r_3 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
select * from cr2r_1;
select * from cr2r_2;
select * from cr2r_3;

insert into cr2c_1 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into cr2c_2 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
insert into cr2c_3 values (4, 5, 6), (5, 6, 7), (6, 7, 8);
select * from cr2c_1;
select * from cr2c_2;
select * from cr2c_3;
--enable_all_result_log
