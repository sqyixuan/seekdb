--disable_warnings
--error 0, 942
drop table literals_t1;
--error 0, 942
drop view literals_v1;
--enable_warnings

eval select $literal_str, -($literal_str) c1 from dual;

eval create view literals_v1 as select $literal_str c1 from dual;
desc literals_v1;
--source mysql_test/include/show_create_table_old_version_replica2.inc
show create view literals_v1;
select * from literals_v1;

eval create table literals_t1 (c1 int, c2 binary_double default $literal_str, c3 binary_float default $literal_str, c4 number(30, 20) default $literal_str, c5 float default $literal_str, c6 float(120) default $literal_str);
desc literals_t1;
--source mysql_test/include/show_create_table_old_version_replica2.inc
show create table literals_t1;

insert into literals_t1 (c1) values (1);
select * from literals_t1;
