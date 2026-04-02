--disable_warnings
--error 0,942
drop table gbktable CASCADE CONSTRAINTS purge;
--enable_warnings

create table gbktable
(
c0 int,
c1 nchar(200),
c2 nchar(200),
c3 blob,
c4 clob,
c5 date,
c6 timestamp,
c7 char(200),
c8 raw(200),
c9 number(10,3),
CONSTRAINT PK_test2 PRIMARY KEY (c1,c2)
);

insert into gbktable values(1,'中文测试001','中文测试001','aaabbb','中文测试1',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'a','bb',100.2);
insert into gbktable values(2,'中文测试002','中文测试002','aaabbb','中文测试002',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'c','ee',100);
insert into gbktable values(3,'中文测试003','中文测试001','aaabbb','中文测试003',to_date('2006-06-04','yyyy-mm-dd'),to_timestamp('20060604', 'yyyymmdd'),'d','ff',100.3);

select * from gbktable t where decode(t.c1,'中文测试001','好的','aaabbb','不好','20060604','再说') = '好的';
select * from gbktable t where replace(to_nchar(t.c1),'中文测试00','好的好的') = '好的好的1';
select * from gbktable t where length(t.c1) = 7;
select * from gbktable t where concat(t.c1,t.c2) = '中文测试001中文测试001' and concat(t.c1,t.c2) = concat(t.c2,t.c1);

--disable_warnings
--error 0,942
DROP TABLE temp_fee CASCADE CONSTRAINTS purge;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings

create table temp_fee(
month_id varchar2(6),
prov_id varchar2(3),
device_number varchar2(40))
partition by range(month_id)
subpartition by hash(prov_id) subpartitions 4
(partition part201606 values less than('201708'),
partition part201607 values less than('201709'),
partition part201608 values less than('201710'),
partition part201609 values less than('201711'));

insert into temp_fee values (201710,101,1111);
insert into temp_fee values (201708,102,2222);
insert into temp_fee values (201709,103,3333);

create index idx_b on temp_fee(month_id) local;
create index idx_c on temp_fee(prov_id) global;
select temp_fee.month_id from temp_fee;