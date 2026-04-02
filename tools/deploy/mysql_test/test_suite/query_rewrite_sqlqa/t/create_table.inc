# owner: luofan.zp
# owner group: sql-sql2
# description: Query Rewrite - 测试环境表结构
# tags: ddl,dml,pl
# author: whh01047778


purge recyclebin;
--result_format 4

# mysql 模式表结构
if ($TENANT == 'mysql') {
--echo >>>>>>>>>>>>>>>>>>>>>>>>>>> qr-mysql-4 测试的表对象	 <<<<<<<<<<<<<<<<<<<<<<<<<
--error 0,1051
drop table t1;
create table t1 (
  id int not null primary key,
  c1_char char(20),
  c2_vcha varchar(20),
  c3_text text,
  c4_enum enum('枚举','enum1','enum2','enum3') default null,
  c5_set1 set('集合','set1','set2','set3','set4') default null,
  index index1 (c1_char)
)charset = utf8mb4 partition by hash(id) partitions 3;

insert into t1 values(1,'char1','varchar1','text1','枚举','集合');
insert into t1 values(2,'char2','varchar2','text2','enum1','set1');
insert into t1 values(3,'char3','varchar3','text3','enum2','set2');
insert into t1 values(4,'char4','varchar4','text4','enum3','set3,set4');

--error 0,1051
drop table t2;
create table t2 (
  id int,
  c1_tint tinyint,
  c2_sint smallint,
  c3_mint mediumint,
  c4_bint bigint,
  c5_floa float,
  c6_doub double,
  c7_deci decimal,
  primary key(id,c1_tint),
  index index2 (id,c1_tint)
)partition by range (c1_tint) ( 
 partition p0 values less than (10), 
 partition p1 values less than (20), 
 partition p2 values less than maxvalue);

insert into t2 values(1,11,12,13,14,15.1,16.5,17.12);
insert into t2 values(2,21,22,23,24,25.1,26.5,27.12);
insert into t2 values(3,31,32,33,34,35.1,36.5,37.12);
insert into t2 values(4,41,42,43,44,45.1,46.5,47.12);
insert into t2 values(5,41,52,53,54,55.1,56.5,57.12);
insert into t2 values(8,81,82,83,84,85.1,86.5,87.12);

--error 0,1051
drop table t3;
create table t3 (
  id int not null,
  c1_year year default '2022',
  c2_time time,
  c3_date date,
  c4_dtim datetime,
  c5_tstam timestamp,
  primary key(id,c1_year,c3_date),
  unique index index3 (id,c1_year,c3_date)
)partition by list(dayofweek(c3_date))(
 partition p0 values in (1,2),
 partition p1 values in (3,4),
 partition p2 values in (5,6,7));

insert into t3 values(1,'2021','11:00:00','2021-10-01','2021-10-01 11:00:00','2021-10-01 11:00:00.123');
insert into t3 values(2,'2022','12:00:00','2022-10-02','2022-10-02 12:00:00','2022-10-02 12:00:00.123');
insert into t3 values(3,'2023','13:00:00','2023-10-03','2023-10-03 13:00:00','2023-10-03 13:00:00.123');
insert into t3 values(4,'2024','14:00:00','2024-10-03','2024-10-03 14:00:00','2024-10-04 14:00:00.123');

--error 0,1051
drop table t4;
create table t4 (
  id int not null,
  c1_tint tinyint,
  c2_vcha varchar(200),
  primary key (id,c1_tint)
)charset = utf8mb4 partition by key() partitions 3;

insert into t4 values(1,10,'查询');
insert into t4 values(2,20,'重写');
insert into t4 values(3,30,'测试');
insert into t4 values(4,40,'用例');
insert into t4 values(5,50,'mysql');
insert into t4 values(7,70,'table t4');

--error 0,1051
drop table t5;
create table t5 (
  id int not null,
  c1_tint tinyint,
  c2_vcha varchar(200),
  primary key (id,c1_tint)
)charset = utf8mb4 partition by key() partitions 3;

insert into t5 values(1,10,'查询');
insert into t5 values(2,20,'重写2');
insert into t5 values(3,30,'测试2');
insert into t5 values(5,50,'mysql');
insert into t5 values(6,60,'table t4');


--error 0,1051
drop view v1;
create view v1 as select * from t2;
 
}

# oracle 模式表结构
if ($TENANT == 'oracle') {
--echo >>>>>>>>>>>>>>>>>>>>>>>>>>> qr-oracle-4 测试的表对象	 <<<<<<<<<<<<<<<<<<<<<<<<<
--error 0,00942
drop table t1;
create table t1 (
  id int not null primary key,
  c1_char char(200) default 'char',
  c2_nchar nchar(200),
  c3_nvchar2 nvarchar2(200),
  c4_vchar2 varchar2(200),
  c5_vchar varchar(200)
)partition by hash(id) partitions 3;

--error 0,01418
drop index index1;
create index index1 on t1 (c1_char);

insert into t1 values(1,'char1','nchar1','nvarchar2','varchar21','varchar1');
insert into t1 values(2,'char2','nchar2','nvarchar2','varchar22','varchar2');
insert into t1 values(3,'char3','nchar3','nvarchar3','varchar23','varchar3');
insert into t1 values(4,'char4','nchar4','nvarchar4','varchar24','varchar4');


--error 0,00942
drop table t2;
create table t2 (
  id int,
  c1_dec dec default 1,
  c2_ric numeric,
  c3_int int,
  c4_ger integer,
  c5_sma smallint,
  c6_flo float,
  c7_rea real,
  c8_num number(8,2),
  primary key(id,c1_dec)
)partition by range (c1_dec) ( 
 partition p0 values less than (10), 
 partition p1 values less than (20), 
 partition p2 values less than (maxvalue));

--error 0,01418
drop index index2;
create index index2 on t2 (c2_ric,c3_int);

insert into t2 values(1,11,12,13,14,15.1,16.5,17.12,18.123);
insert into t2 values(2,21,22,23,24,25.1,26.5,27.12,18.123);
insert into t2 values(3,31,32,33,34,35.1,36.5,37.12,18.123);
insert into t2 values(4,41,42,43,44,45.1,46.5,47.12,18.123);
insert into t2 values(5,51,52,53,54,55.1,56.5,57.12,58.123);
insert into t2 values(8,81,82,83,84,85.1,86.5,87.12,88.123);


--error 0,00942
drop table t3;
create table t3 (
  id int not null primary key,
  c1_date1 date,
  c2_times timestamp,
  c3_timez timestamp with time zone,
  c4_local timestamp with local time zone,
  c5_intds interval day to second,
  c6_intym interval year to month
)partition by list(id)(
 partition p0 values (1,2),
 partition p1 values (3,4),
 partition p2 values (5,6,7));

--error 0,01418
drop index index3;
create unique index index3 on t3 (id,c1_date1,c2_times);

insert into t3 values(1,'2022-11-01 11:00:00',timestamp'2022-11-01 11:00:00.123 +12:00',timestamp'2022-11-01 11:00:00.123456 +13:00',timestamp'2022-11-01 11:00:00.123456789 +14:00','+11 00:00:00.000000','+01-00');
insert into t3 values(2,'2022-11-02 12:00:00',timestamp'2022-11-02 12:00:00.123 +12:00',timestamp'2022-11-02 12:00:00.123456 +13:00',timestamp'2022-11-02 12:00:00.123456789 +14:00','+12 00:00:00.000000','+02-00');
insert into t3 values(3,'2022-11-03 13:00:00',timestamp'2022-11-03 13:00:00.123 +12:00',timestamp'2022-11-03 13:00:00.123456 +13:00',timestamp'2022-11-03 13:00:00.123456789 +14:00','+13 00:00:00.000000','+03-00');
insert into t3 values(4,'2022-11-04 14:00:00',timestamp'2022-11-04 14:00:00.123 +12:00',timestamp'2022-11-04 14:00:00.123456 +13:00',timestamp'2022-11-04 14:00:00.123456789 +14:00','+14 00:00:00.000000','+04-00');


--error 0,00942
drop table t4;
create table t4 (
  id int not null,
  c1_inte integer,
  c2_nchar varchar2(200),
  primary key (id,c1_inte)
);

--error 0,01418
drop index index4;
create index index4 on t4(c1_inte) global
 partition by range(c1_inte)(
 partition p0 values less than (10), 
 partition p1 values less than (20), 
 partition p2 values less than (maxvalue)
);

insert into t4 values(1,10,'查询');
insert into t4 values(2,20,'重写');
insert into t4 values(3,30,'测试');
insert into t4 values(4,40,'用例');
insert into t4 values(5,50,'oracle');
insert into t4 values(7,70,'table t4');


--error 0,00942
drop table t5;
create table t5 (
  id int not null,
  c1_inte integer,
  c2_nchar varchar2(200),
  primary key (id,c1_inte)
);

--error 0,01418
drop index index5;
create index index5 on t5(c1_inte) global
 partition by range(c1_inte)(
 partition p0 values less than (10), 
 partition p1 values less than (20), 
 partition p2 values less than (maxvalue)
);
 
insert into t5 values(1,10,'查询');
insert into t5 values(2,20,'重写2');
insert into t5 values(3,30,'测试2');
insert into t5 values(5,50,'oracle');
insert into t5 values(6,60,'table t5');

--error 0,00942
drop view v1;
create view v1 as select * from t2;

}
