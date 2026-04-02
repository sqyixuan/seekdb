
create table tbl_number (col_number number(30, 10), col_varchar varchar2(40), col_char char(40));
create table tbl_number_str (col_varchar varchar2(40), col_char char(40));
create table tbl_datetime (col_date date, col_timestamp timestamp, col_varchar varchar2(40), col_char char(40));
create table tbl_datetime_str (col_varchar varchar2(40), col_char char(40));
create table tbl_string (col_varchar varchar2(40), col_char char(40));

sleep 1;

insert into tbl_number(col_number) values (-99999999999999999999.9999999999);
insert into tbl_number(col_number) values (-123456789);
insert into tbl_number(col_number) values (-123.45678900);
insert into tbl_number(col_number) values (-0.0000000001);
insert into tbl_number(col_number) values (0);
insert into tbl_number(col_number) values (0.0000000001);
insert into tbl_number(col_number) values (987.654321);
insert into tbl_number(col_number) values (987654321);
insert into tbl_number(col_number) values (99999999999999999999.9999999999);
insert into tbl_number(col_number) values (NULL);
update tbl_number set col_varchar = col_number;
update tbl_number set col_char = col_number;
commit;
select * from tbl_number order by col_number;

insert into tbl_number_str(col_varchar) values ('  -0099999999999999999999.999999999900  ');
insert into tbl_number_str(col_varchar) values ('  -00123456789.00  ');
insert into tbl_number_str(col_varchar) values ('  -00123.45678900  ');
insert into tbl_number_str(col_varchar) values ('  -000.000000000100');
insert into tbl_number_str(col_varchar) values ('  000.00  ');
insert into tbl_number_str(col_varchar) values ('  000.000000000100  ');
insert into tbl_number_str(col_varchar) values ('  00987.65432100  ');
insert into tbl_number_str(col_varchar) values ('  00987654321.00  ');
insert into tbl_number_str(col_varchar) values ('  0099999999999999999999.999999999900  ');
insert into tbl_number_str(col_varchar) values (NULL);
update tbl_number_str set col_char = col_varchar;
commit;
select * from tbl_number_str order by col_varchar;

insert into tbl_datetime(col_timestamp) values ('1000-01-01 00:00:00');
insert into tbl_datetime(col_timestamp) values ('1234-12-23 02:15:16');
insert into tbl_datetime(col_timestamp) values ('1799-06-18 06:07:31');
insert into tbl_datetime(col_timestamp) values ('1970-01-01 00:00:00');
insert into tbl_datetime(col_timestamp) values ('2015-10-16 15:34:21');
insert into tbl_datetime(col_timestamp) values ('2037-01-01 22:20:17');
insert into tbl_datetime(col_timestamp) values ('3296-10-30 10:46:38');
insert into tbl_datetime(col_timestamp) values ('5555-05-05 05:05:05.555555');
insert into tbl_datetime(col_timestamp) values ('7777-07-07 07:07:07.777777');
insert into tbl_datetime(col_timestamp) values ('9999-12-31 23:59:59.999999');
insert into tbl_datetime(col_timestamp) values (NULL);
update tbl_datetime set col_date = col_timestamp;
update tbl_datetime set col_varchar = col_date;
update tbl_datetime set col_char = col_date;
commit;
# 
# update result for all test base on prepare_data.sql after the bug above is fixed.
select * from tbl_datetime order by col_timestamp;

insert into tbl_datetime_str(col_varchar) values ('  1000/01#01    00$00%00  ');
insert into tbl_datetime_str(col_varchar) values ('  1234/12#23    02$15%16  ');
insert into tbl_datetime_str(col_varchar) values ('  1799/06#18    06$07%31  ');
insert into tbl_datetime_str(col_varchar) values ('  1970/01#01    00$00%00  ');
insert into tbl_datetime_str(col_varchar) values ('  2015/10#16    15$34%21  ');
insert into tbl_datetime_str(col_varchar) values ('  2037/01#01    22$20%17  ');
insert into tbl_datetime_str(col_varchar) values ('  3296/10#30    10$46%38  ');
insert into tbl_datetime_str(col_varchar) values ('  5555-05-05    05:05:05  ');
insert into tbl_datetime_str(col_varchar) values ('  7777-07-07    07:07:07  ');
insert into tbl_datetime_str(col_varchar) values ('  9999-12-31    23:59:59  ');
insert into tbl_datetime_str(col_varchar) values (NULL);
update tbl_datetime_str set col_char = col_varchar;
commit;
select * from tbl_datetime_str order by col_varchar;

insert into tbl_string(col_varchar) values ('-123.00a');
insert into tbl_string(col_varchar) values ('a987654321');
insert into tbl_string(col_varchar) values ('a987654321  ');
insert into tbl_string(col_varchar) values ('sql');
insert into tbl_string(col_varchar) values ('SQL');
insert into tbl_string(col_varchar) values ('SQL  ');
insert into tbl_string(col_varchar) values ('storage');
insert into tbl_string(col_varchar) values ('STORAGE');
insert into tbl_string(col_varchar) values ('STORAGE  ');
insert into tbl_string(col_varchar) values ('transaction');
insert into tbl_string(col_varchar) values ('TRANSACTION');
insert into tbl_string(col_varchar) values ('TRANSACTION  ');
insert into tbl_string(col_varchar) values ('阿里巴巴');
insert into tbl_string(col_varchar) values ('蚂蚁金服');
insert into tbl_string(col_varchar) values ('蚂蚁金服  ');
insert into tbl_string(col_varchar) values (NULL);
update tbl_string set col_char = col_varchar;
commit;
select * from tbl_string order by col_varchar;

