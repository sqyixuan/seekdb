purge recyclebin;

####分区
###hash分区
--disable_warnings
--error 0,942
DROP TABLE t1;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings

create table t1(a nvarchar2(200) , b varchar2(200), c nvarchar2(200), d varchar2(200)) partition by hash(a) partitions 4;
create index idx_b on t1(b);
create index idx_c on t1(c) global partition by hash(c) partitions 4;
insert into t1 values (1,1,1,1);
insert into t1 values ('中文测试1','中文测试1','中文测试1','中文测试1');
select /*+use_index(t1 idx_b)*/ t1.b from t1;
select /*+use_index(t1 idx_b)*/ t1.b from t1 where t1.b = '中文测试1';
select /*+use_index(t1 idx_b)*/ t1.b from t1 where t1.c = '中文测试1';


--disable_warnings
--error 0,942
DROP TABLE t1;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings

###范围分区(特别要注意的是"范围"中不包含=)####

--disable_warnings
--error 0,942
DROP TABLE temp_fee;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings

###单范围
create table temp_fee(
month_id nvarchar2(60),
prov_id varchar2(30),
device_number varchar2(40))
partition by range (month_id)
(partition part201606 values less than('201708'),
partition part201607 values less than('201709'),
partition part201608 values less than('201710'),
partition part201609 values less than('201711')
);

create index idx_b on temp_fee(month_id);
create index idx_c on temp_fee(prov_id) global;
insert into temp_fee values (201710,101,1111);
insert into temp_fee values (201708,'中文测试1','中文测试1');
insert into temp_fee values (201709,'中文测试2','中文测试2');
--error 14400,600
insert into temp_fee values ('中文测试3','中文测试2','中文测试2');
select /*+use_index(temp_fee idx_b)*/ temp_fee.month_id from temp_fee;
select /*+use_index(temp_fee idx_c)*/ temp_fee.month_id from temp_fee;
###匹配非分区字段
select /*+use_index(temp_fee idx_b)*/ temp_fee.month_id from temp_fee where temp_fee.prov_id = '中文测试1';
select /*+use_index(temp_fee idx_c)*/ temp_fee.month_id from temp_fee where temp_fee.prov_id = '中文测试2';
select /*+use_index(temp_fee idx_b)*/ temp_fee.month_id from temp_fee where temp_fee.month_id = '中文测试1';
select /*+use_index(temp_fee idx_c)*/ temp_fee.month_id from temp_fee where temp_fee.device_number = '中文测试2';

--disable_warnings
--error 0,942
DROP TABLE temp_fee;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings


####组合分区 range-list####
--echo  ##场景测试gb18030-6//二级分区gb18030-6-1//gb18030-6-1-1
--disable_warnings
--error 0,942
DROP TABLE temp_fee;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings
###二级分区语法不一致
###create table temp_fee(
###month_id varchar2(6),
###prov_id varchar2(3),
###device_number varchar2(40))
###partition by range(month_id)
###subpartition by list (prov_id)
###(partition part201606 values less than ('201711')
###(subpartition sys_default values (default)));

create table temp_fee(
month_id nvarchar2(60),
prov_id varchar2(30),
device_number varchar2(40))
partition by range(month_id)
subpartition by list (prov_id)
subpartition template
(subpartition sys_default values (default))
(partition part201606 values less than ('201711'));

create index idx_b on temp_fee(month_id);
create index idx_c on temp_fee(prov_id) global;
insert into temp_fee values (201710,101,1111);
insert into temp_fee values (201708,'中文测试1','中文测试1');
insert into temp_fee values (201709,'中文测试2','中文测试2');
select /*+use_index(temp_fee idx_b)*/ temp_fee.month_id from temp_fee;
select /*+use_index(temp_fee idx_c)*/ temp_fee.month_id from temp_fee;

select /*+use_index(temp_fee idx_c)*/ * from temp_fee where temp_fee.prov_id = '中文测试1';
select /*+use_index(temp_fee idx_c)*/ * from temp_fee where temp_fee.device_number = '中文测试2';
--disable_warnings
--error 0,942
DROP TABLE temp_fee;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings

--echo  ##场景测试gb18030-6//二级分区gb18030-6-1//gb18030-6-1-2

create table temp_fee(
month_id varchar2(6),
prov_id varchar2(3),
device_number varchar2(40))
partition by list(prov_id)
subpartition by hash(month_id) subpartitions 4
(partition sys_default values (default));

create index idx_b on temp_fee(month_id);
create index idx_c on temp_fee(prov_id) global;
insert into temp_fee values (201710,101,1111);
select /*+use_index(temp_fee idx_b)*/ temp_fee.month_id from temp_fee;
select /*+use_index(temp_fee idx_c)*/ temp_fee.month_id from temp_fee;

--disable_warnings
--error 0,942
DROP TABLE temp_fee;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings


create table temp_fee(
month_id varchar2(6),
prov_id varchar2(3),
device_number varchar2(40))
partition by list(prov_id)
subpartition by hash(month_id) subpartitions 4
(partition sys_default values ('aaa'));

create index idx_b on temp_fee(month_id);
create index idx_c on temp_fee(prov_id) global;
--error 14400,600
insert into temp_fee values (201710,101,1111);
select /*+use_index(temp_fee idx_b)*/ temp_fee.month_id from temp_fee;
select /*+use_index(temp_fee idx_c)*/ temp_fee.month_id from temp_fee;

--disable_warnings
--error 0,942
DROP TABLE temp_fee;
--error 0,01418
drop index idx_b;
--error 0,01418
drop index idx_c;
--enable_warnings

--disable_warnings
--error 0,942
DROP TABLE tt_utf16;
--enable_warnings

create table tt_utf16 
(c1 nvarchar2(200),c2 varchar2(200)) 
partition by hash(c1) 
subpartition by list(c2) SUBPARTITION TEMPLATE 
(SUBPARTITION t_utf16_p0 VALUES ('aaaaa','a','aa','aaa','aaaa')) partitions 3;

--disable_warnings
--error 0,942
DROP TABLE tt_utf16;
--enable_warnings

CREATE TABLE tt_utf16(
C1 CHAR(100),
C2 varchar2(50),
C3 VARCHAR2(20),
C4 NVARCHAR2(50)
);

insert into tt_utf16 values (1,1,1,1);
insert into tt_utf16 values (2,2,2,2);
insert into tt_utf16 values (3,3,3,3);
insert into tt_utf16 values (4,4,4,4);

select * from tt_utf16 where c1='2' or c2='1';
select * from tt_utf16 where c1 between '0' and '5';

--disable_warnings
--error 0,942
DROP TABLE tt_utf16;
--enable_warnings


--echo  ##################复制表操作, 约束, 分区##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--error 0,942
DROP TABLE stu3;
--enable_warnings

--echo  # 创建两张关联表, 用上所有约束, 添加索引, 用来作为复制表的对象
create table stu1(s1 varchar2(2) primary key, s2 varchar2(20) default 'default') compress;
comment on table stu1 is 'default';
comment on column stu1.s1 is 'foreign key';
insert into stu1(s1) values(1);
insert into stu1(s1) values(2);
insert into stu1(s1) values(3);
insert into stu1(s1) values(4);
###--error 00001
###insert into stu1(s1) values(2);


create table stu2(s1 varchar2(2) unique not null, s2 varchar2(20), s3 varchar2(20) default 1, constraint stu2_all_pk primary key(s1,s2,s3) using index, constraint stu2_ch check(s2>5 and s3<3), constraint stu2_fk foreign key(s3) references stu1(s1)) compress partition by hash(s1,s2) partitions 20;
##comment on table stu2 is '关联表';
##comment on column stu2.s1 is '分区字段';
##comment on column stu2.s2 is 'check约束字段, 必须大于5, 与s1一起作为hash分区的列';
##comment on column stu2.s3 is 'foreign key关联stu1表s1字段';
alter table stu2 enable row movement;


--echo  # 添加索引
--error 01408
create index s1_ind on stu2(s1);
create index s2_ind on stu2(s2);
create index s3_ind on stu2(s3);


commit;
--echo  # 添加并更新正常值
--disable_result_log
insert into stu2(s1,s2,s3) values(1,6,1);
###--error 00001
###insert into stu2(s1,s2) values(1,6);

update stu2 set s3=2 where s1=1;

--echo  # 更新为check限制外的值
--error 02290
update stu2 set s3=3 where s1=1;

--echo  # 更新为不存在的关联值
--error 02290
update stu2 set s3=6 where s1=1;
--enable_result_log

insert into stu2(s1,s2) values(2,7);

--echo  # 检查unique是否有效
--disable_result_log
--error 00001
insert into stu2(s1,s2) values(2,8);
--error 00001
update stu2 set s1=1 where s1=2;
--enable_result_log

--echo  # 检查not null是否有效
--error 01400
insert into stu2(s2) values(8);


--echo  # insert 不存在的关联值
--error 02290,2291
insert into stu2(s1,s2,s3) values(3,8,6);

--echo  # s2字段的check是否有效,需大于5
--error 02290,2291
insert into stu2(s1,s2,s3) values(4,5,2);
##--error 02290
##update stu2 set s2=4 where s1=1;


--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--error 0,942
DROP TABLE stu3;
--enable_warnings

create table stu1(s1 varchar2(2) primary key, s2 varchar2(20) default 'default') compress;
##comment on table stu1 is '被关联表';
##comment on column stu1.s1 is '被foreign key 关联的字段';
insert into stu1(s1) values(1);
insert into stu1(s1) values(2);
insert into stu1(s1) values(3);
insert into stu1(s1) values(4);

create table stu2(s1 varchar2(2) unique not null, s2 varchar2(20), s3 varchar2(20) default 1, constraint stu2_all_pk primary key(s1,s2,s3) using index, constraint stu2_ch check(s2>5 and s3<3));
##comment on table stu2 is '关联表';
##comment on column stu2.s1 is '分区字段';
##comment on column stu2.s2 is 'check约束字段, 必须大于5, 与s1一起作为hash分区的列';
##comment on column stu2.s3 is 'foreign key关联stu1表s1字段';
alter table stu2 enable row movement;


--echo  # 添加索引
create index s2_ind on stu2(s2);
create index s3_ind on stu2(s3);


--echo  # 插入些正常值供后续使用
insert into stu2(s1,s2,s3) values(1,6,1);
insert into stu2 values(6, 8, 1);
insert into stu2 values(7, 9, 2);
insert into stu2 values(8, 10, 1);
select * from stu2;


--echo  ######复制表操作(create...as...)######
#create table stu(s1,s2,s3) as select * from stu2;
create table stu as select * from stu2;
###select * from stu;
desc stu;

--echo  # 依次测试unique, not null, check, foreign key, primary key是否有效
insert into stu values(1,11,1);
--error 01400
insert into stu values('',11,1);
insert into stu values(1,2,1);
insert into stu values(1,2,10);
insert into stu values(1,2,10);


--echo  # sql中验证函数的使用//cast  类型的转换
select cast(s3 as number), count(*), sum(s2) from stu where 1=1 group by cast(s3 as number) having cast(s3 as number) < 20 order by cast(s3 as number) desc;
select to_number(s1), nvl(s2, null), decode(s3, 1, '一', 10, '十', '其它') from stu;

--echo  # 复制表中s1的null限制还存在(insert), 验证update
--error 01400
update stu set s1='' where s3=10;
update stu set s1=1 where s2=10;
update stu set s2=1 where s3=2;
##--error 01400
##update stu set s2='' where s3=2;
update stu set s3=100 where s1=1;
##--error 01400
##update stu set s3='' where s1=1;

--echo  # 复制表操作(insert...as...)
create table stu3(s1 number, s2 varchar2(100), s3 varchar2(100));
insert into stu3 select * from stu2;

--echo  # 验证复制来的表是否受限
insert into stu3 values(66666, '010203040506070809101', 15);
insert into stu3 values('','','');

--echo  # sql中验证函数的使用
select cast(s3 as number), count(*), sum(s2) from stu3 where 1=1 group by cast(s3 as number) having cast(s3 as number) < 20 order by cast(s3 as number) desc;
select to_number(s1), nvl(s2, null), decode(s3, 1, '一', 10, '十', '其它') from stu3;

--echo  # 不支持禁用约束, 删除约束, 写入值测试
alter table stu2 drop constraint stu2_ch;
--disable_result_log
--error 00001
insert into stu2 values(1, 2, 2);
--enable_result_log
insert into stu2 values(10, 2, 2);
insert into stu2 values(11, 2, 10);

--echo  # delete数据并查看stu2, stu, stu3
delete from stu2 where s3=2;
delete from stu where s3='';
delete from stu3 where s3=1;
select * from stu2;
###select * from stu;
select s1,s3 from stu3;

truncate table stu2;
insert into stu2(s1,s2,s3) values(1,6,1);

--echo  ##################约束, 分区(range)##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--error 0,942
DROP TABLE stu3;
--enable_warnings

--echo  # 创建两张关联表, 用上所有约束, 添加索引
create table stu1(s1 varchar2(2) primary key, s2 varchar2(20) default 'val') compress;
##comment on table stu1 is '被关联表';
##comment on column stu1.s1 is '被foreign key 关联的字段';
insert into stu1(s1) values(1);
insert into stu1(s1) values(2);
insert into stu1(s1) values(3);
insert into stu1(s1) values(4);
insert into stu1(s1) values(5);
insert into stu1(s1) values(6);


--echo  # range分区, 用s3列varchar2类型

##create table stu2(s1 varchar2(2) unique not null, s2 varchar2(20), s3 varchar2(20) default 1, constraint stu2_all_pk primary key(s1,s2,s3) using index, constraint stu2_ch check(s2>5 and s3<5)) partition by range(s3)(partition s3_p1 values less than(2), partition s3_p2 values less than(3), partition s3_pmax values less than (maxvalue)) compress;

create table stu2(s1 varchar2(2) unique not null, s2 varchar2(20), s3 varchar2(20) default 1, constraint stu2_all_pk primary key(s1,s2,s3) using index, constraint stu2_ch check(s2>5 and s3<5)) compress partition by range(s3)(partition s3_p1 values less than(2), partition s3_p2 values less than(3), partition s3_pmax values less than (maxvalue));

##create table stu2(s1 varchar2(2) unique not null, s2 varchar2(20), s3 varchar2(20) default 1, constraint stu2_all_pk primary key(s1,s2,s3) using index, constraint stu2_ch check(s2>5 and s3<5)) partition by range(s3)(partition s3_p1 values less than(2), partition s3_p2 values less than(3), partition s3_pmax values less than (maxvalue));

##comment on table stu2 is '关联表';
##comment on column stu2.s1 is '非空唯一';
##comment on column stu2.s2 is 'check约束字段, 须大于5';
##comment on column stu2.s3 is 'foreign key关联stu1表s1字段, 也是range的分区字段';
alter table stu2 enable row movement;

--echo  # 添加索引
--error 01408
create index s1_ind on stu2(s1);
create index s2_ind on stu2(s2);
create index s3_ind on stu2(s3);


--echo  # 插入正确数据, 读取内容, 先清空原数据
select * from stu2;
delete from stu2 where s1=3;
select * from stu2;
delete from stu2 where s1=2;
select * from stu2;
delete from stu2;
select * from stu2;
insert into stu2(s1,s2) values(1, 20191001);
--echo  # 世纪闰年 求周二
insert into stu2(s1,s2,s3) values(2, '20000228', 2);
--echo  # 世纪平年 求周一
insert into stu2(s1,s2) values(3, '21000228');
--error 01722
insert into stu2(s1,s2) values(4, '2019-10-01');
--error 01722
insert into stu2(s1,s2) values(5, '2019/01/29');
--echo  # 闰年, 求周五
insert into stu2(s1,s2,s3) values(6, '20080222', 2);


--echo  # sql中验证函数的使用//cast  类型的转换
select cast(s3 as number), count(*), sum(s2) from stu2 where 1=1 group by cast(s3 as number) having cast(s3 as number) < 20 order by cast(s3 as number) desc;

###select to_number(s1), nvl(s2, null), decode(s3, 1, '一', 10, '十', '其它') from stu2;

###select * from stu2;
##select cast(s3 as number)-10 as s1, last_day(s2) ls2, decode(s2, 20000228, next_day(to_date(to_char(s2,99999999),'yyyymmdd'),'Monday')) ds2 from stu2;
##select last_day(s2) ls2 from stu2;

##select cast(s3 as number)-10 as s1, last_day(s2) ls2, decode(s2, 20000228, next_day(to_date(s2,'yyyymmdd'),'Tuesday'), 21000228, next_day(to_date(s2,'yyyymmdd'),'Monday'), 20080222, next_day(to_date(s2,'yyyymmdd'),'Friday'), next_day(to_date(s2,'yyyymmdd'),'Saturday ')) ds2 from stu2;
###select decode(s2, 20000228, next_day(to_date(s2,'yyyymmdd'),'Tuesday'), 21000228, next_day(to_date(s2,'yyyymmdd'),'Monday'), 20080222, next_day(to_date(s2,'yyyymmdd'),'Friday'), next_day(to_date(s2,'yyyymmdd'),'Saturday ')) ds2 from stu2;

###select cast(s3 as number)-10 as s1, last_day(s2) ls2, decode(s2, 20000228, next_day(s2,'Tuesday'), 21000228, next_day(s2,'Monday'), 20080222, next_day(s2,'Friday'), next_day(s2,'Saturday ')) ds2 from stu2;
###select cast(s3 as number)-10 as s1,  decode(s2, 20000228, next_day(s2,'Tuesday'), 21000228, next_day(s2,'Monday'), 20080222, next_day(s2,'Friday'), next_day(s2,'Saturday ')) ds2 from stu2;


--echo  ##################约束, 分区(list)##################
--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--error 0,942
DROP TABLE stu3;
--enable_warnings

--echo  # 创建两张关联表, 用上所有约束, 添加索引
create table stu1(s1 varchar2(2) primary key, s2 varchar2(20) default 'val') compress;
##comment on table stu1 is '被关联表';
##comment on column stu1.s1 is '被foreign key 关联的字段';
insert into stu1(s1) values(1);
insert into stu1(s1) values(2);
insert into stu1(s1) values(3);
insert into stu1(s1) values(4);
insert into stu1(s1) values(5);
insert into stu1(s1) values(6);

--echo  # list分区, 用s3列nchar类型
create table stu2(s1 varchar2(2) unique not null, s2 varchar2(20), s3 varchar2(20) default 1, constraint stu2_all_pk primary key(s1,s2,s3) using index, constraint stu2_ch check(s2>5 and s3<5)) compress partition by list(s3)(partition s1_p1 values(1), partition s1_p2 values(2), partition s1_p3 values(3), partition s1_at values(default));
##comment on table stu2 is '关联表';
##comment on column stu2.s1 is 's1非空唯一';
##comment on column stu2.s2 is 'check约束字段, 须大于5';
##comment on column stu2.s3 is 'foreign key关联stu1表s1字段, 也是list的分区字段';
alter table stu2 enable row movement;

--echo  # 添加索引
--error 01408
create index s1_ind on stu2(s1);
--error 0,1418
drop index s2_ind;
create index s2_ind on stu2(s2);

--echo  # 创建global索引, 全局索引不支持使用list分区
--error 1408,14151
create index ss2_ind on stu2(s2) global partition by list(s3)(partition s1_p1 values(1), partition s1_p2 values(2), partition s1_p3 values(3), partition s1_at values(default));
--error 0,1418
drop index s2_ind;
create index ss3_ind on stu2(s3) global partition by range(s3)(partition s_n1_p1 values less than(2), partition s_n1_p2 values less than(3), partition s_n1_p3 values less than(4), partition s_n1_at values less than(maxvalue));


###--echo  # 压缩分区表s1_p1分区
###alter table stu2 move partition s1_p1 compress for oltp;

--echo  # 删除其中一个list分区s1_p1, 查看该分区数据是否还存在
insert into stu2(s1,s2) values(10, 16);
insert into stu2(s1,s2) values(11, 17);
insert into stu2 values(12, 18, 2);
insert into stu2 values(13, 19, 3);
insert into stu2 values(14, 20, 4);
select * from stu2;

--disable_warnings
--error 0,942
DROP TABLE stu;
--error 0,942
DROP TABLE stu2;
--error 0,942
DROP TABLE stu1;
--error 0,942
DROP TABLE stu3;
--enable_warnings


--echo  ##################中文, 日语, 韩语等的测试##################
--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop type my_type;
--enable_warnings

--echo  # 验证预期建表失败-生成列需大于依赖列字符数加拼接字符数
--error 600,12899
create table stu(s1 varchar2(2), s2 varchar2(2), s3 varchar2(2), s4 varchar2(4) generated always as(s3 || '123'));

create table stu(s1 varchar2(2), s2 varchar2(20), s3 varchar2(20), s4 varchar2(200) generated always as(s3 || '好的'));

insert into stu(s1, s2, s3) values(1, '北京', '朝阳');

insert into stu(s1, s2, s3) values(1, 2, 3);

###select * from stu;

--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop type my_type;
--enable_warnings

create table stu(s1 varchar2(9) primary key, s2 varchar2(9) unique, s3 varchar2(9) not null unique);
create table stu1(s1 nchar(10) primary key, s2 varchar2(10) unique, s3 nvarchar2(10) not null unique);
insert into stu values('韩国', '韓国', '??');
insert into stu values(260, 'abc', '1a?');
insert into stu values(028, '0ab', '01a');

insert into stu values('讋讋讋', '詟詟詟', '詟詟詟');
insert into stu values('???', '???', '???');

--disable_result_log
--error 00001
insert into stu values(028, '0ab', '01a');

--echo  # 验证预期的失败
--error 1,12899
insert into stu values(0028, '00ab', '001a');
--error 1,12899
insert into stu values(012, '韓国', '????');
--error 1,12899
insert into stu values('讋讋讋詟', '詟詟詟', '詟詟詟');

--error 00001
insert into stu values('??', '??', '??');

--enable_result_log

insert into stu values('??', '', '?  ?');
####insert into stu values('中文', '', '再看');

###select * from stu;

delimiter /;
declare
cursor my_cur is select * from stu;
begin
for obj in my_cur loop
insert into stu1 values(obj.s1 || 1, obj.s2 || 2, obj.s3 || 3);
commit;
end loop;
end;
/

delimiter ;/
select * from stu1;

--disable_warnings
--error 0,942
drop table stu;
--error 0,942
drop table stu2;
--error 0,942
drop table stu1;
--error 0,600
drop type my_type;
--enable_warnings


####pk where条件中比较字段查看，判断内容+判断方法
--disable_warnings
--error 0,942
drop table EMPLOYEES;
--enable_warnings

CREATE TABLE EMPLOYEES (
  EMPLOYEE_ID NUMBER(6) NOT NULL,
  FIRST_NAME NVARCHAR2(20),
  LAST_NAME NVARCHAR2(25) NOT NULL,
  EMAIL VARCHAR2(25) NOT NULL,
  PHONE_NUMBER VARCHAR2(20),
  HIRE_DATE DATE NOT NULL,
  JOB_ID VARCHAR2(10) NOT NULL,
  SAL NUMBER(8, 2),
  COMMISSION_PCT NUMBER(2, 2),
  MANAGER_ID NUMBER(6),
  DEPARTMENT_ID NUMBER(4)
);

INSERT INTO employees VALUES ('100', 'Steven', 'King', 'SKING', '515.123.4567', to_date('2006-06-04','yyyy-mm-dd'), 'AD_PRES', '24000.00', null, null, '90');
INSERT INTO employees VALUES ('101', 'Neena', 'Kochhar', 'NKOCHHAR', '515.123.4568', to_date('2006-06-04','yyyy-mm-dd'), 'AD_VP', '17000.00', null, '100', '90');
INSERT INTO employees VALUES ('102', 'Lex', 'De Haan', 'LDEHAAN', '515.123.4569', to_date('2006-06-04','yyyy-mm-dd'), 'AD_VP', '17000.00', null, '100', '90');
INSERT INTO employees VALUES ('103', 'Alexander', 'Hunold', 'AHUNOLD', '590.423.4567', to_date('2006-06-04','yyyy-mm-dd'), 'IT_PROG', '9000.00', null, '102', '60');
INSERT INTO employees VALUES ('104', 'Bruce', 'Ernst', 'BERNST', '590.423.4568', to_date('2006-06-04','yyyy-mm-dd'), 'IT_PROG', '6000.00', null, '103', '60');
INSERT INTO employees VALUES ('105', 'David', 'Austin', 'DAUSTIN', '590.423.4569', to_date('2006-06-04','yyyy-mm-dd'), 'IT_PROG', '4800.00', null, '103', '60');
INSERT INTO employees VALUES ('106', 'Valli', 'Pataballa', 'VPATABAL', '590.423.4560', to_date('2006-06-04','yyyy-mm-dd'), 'IT_PROG', '4800.00', null, '103', '60');
INSERT INTO employees VALUES ('107', 'Diana', 'Lorentz', 'DLORENTZ', '590.423.5567', to_date('2006-06-04','yyyy-mm-dd'), 'IT_PROG', '4200.00', null, '103', '60');
INSERT INTO employees VALUES ('108', 'Nancy', 'Greenberg', 'NGREENBE', '515.124.4569', to_date('2006-06-04','yyyy-mm-dd'), 'FI_MGR', '12000.00', null, '101', '100');
INSERT INTO employees VALUES ('109', 'Daniel', 'Faviet', 'DFAVIET', '515.124.4169', to_date('2006-06-04','yyyy-mm-dd'), 'FI_ACCOUNT', '9000.00', null, '108', '230');
INSERT INTO employees VALUES ('110', 'John', 'Chen', 'JCHEN', '515.124.4269', to_date('2006-06-04','yyyy-mm-dd'), 'FI_ACCOUNT', '8200.00', null, '108', '100');
INSERT INTO employees VALUES ('111', 'Ismael', 'Sciarra', 'ISCIARRA', '515.124.4369', to_date('2006-06-04','yyyy-mm-dd'), 'FI_ACCOUNT', '7700.00', null, '108', '100');
INSERT INTO employees VALUES ('112', 'Jose Manuel', 'Urman', 'JMURMAN', '515.124.4469', to_date('2006-06-04','yyyy-mm-dd'), 'FI_ACCOUNT', '7800.00', null, '108', '100');
INSERT INTO employees VALUES ('113', 'Luis', 'Popp', 'LPOPP', '515.124.4567', to_date('2006-06-04','yyyy-mm-dd'), 'FI_ACCOUNT', '6900.00', null, '108', '100');
INSERT INTO employees VALUES ('114', 'Den', 'Raphaely', 'DRAPHEAL', '515.127.4561',to_date('2006-06-04','yyyy-mm-dd'), 'PU_MAN', '11000.00', null, '100', '30');
INSERT INTO employees VALUES ('115', 'Alexander', 'Khoo', 'AKHOO', '515.127.4562', to_date('2006-06-04','yyyy-mm-dd'), 'PU_CLERK', '3100.00', null, '114', '30');
INSERT INTO employees VALUES ('116', 'Shelli', 'Baida', 'SBAIDA', '515.127.4563', to_date('2006-06-04','yyyy-mm-dd'), 'PU_CLERK', '2900.00', null, '114', '30');
INSERT INTO employees VALUES ('117', 'Sigal', 'Tobias', 'STOBIAS', '515.127.4564',to_date('2006-06-04','yyyy-mm-dd'), 'PU_CLERK', '2800.00', null, '114', '30');
INSERT INTO employees VALUES ('118', 'Guy', 'Himuro', 'GHIMURO', '515.127.4565',to_date('2006-06-04','yyyy-mm-dd'), 'PU_CLERK', '2600.00', null, '114', '30');
INSERT INTO employees VALUES ('119', 'Karen', 'Colmenares', 'KCOLMENA', '515.127.4566',to_date('2006-06-04','yyyy-mm-dd'), 'PU_CLERK', '2500.00', null, '114', '30');
INSERT INTO employees VALUES ('120', 'Matthew', 'Weiss', 'MWEISS', '650.123.1234',to_date('2006-06-04','yyyy-mm-dd'), 'ST_MAN', '8000.00', null, '100', '50');
INSERT INTO employees VALUES ('121', 'Adam', 'Fripp', 'AFRIPP', '650.123.2234', to_date('2006-06-04','yyyy-mm-dd'), 'ST_MAN', '8200.00', null, '100', '50');
INSERT INTO employees VALUES ('122', 'Payam', 'Kaufling', 'PKAUFLIN', '650.123.3234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_MAN', '7900.00', null, '100', '50');
INSERT INTO employees VALUES ('123', 'Shanta', 'Vollman', 'SVOLLMAN', '650.123.4234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_MAN', '6500.00', null, '100', '50');
INSERT INTO employees VALUES ('124', 'Kevin', 'Mourgos', 'KMOURGOS', '650.123.5234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_MAN', '5800.00', null, '100', '50');
INSERT INTO employees VALUES ('125', 'Julia', 'Nayer', 'JNAYER', '650.124.1214',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '3200.00', null, '120', '50');
INSERT INTO employees VALUES ('126', 'Irene', 'Mikkilineni', 'IMIKKILI', '650.124.1224',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2700.00', null, '120', '50');
INSERT INTO employees VALUES ('127', 'James', 'Landry', 'JLANDRY', '650.124.1334',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2400.00', null, '120', '50');
INSERT INTO employees VALUES ('128', 'Steven', 'Markle', 'SMARKLE', '650.124.1434', to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2200.00', null, '120', '50');
INSERT INTO employees VALUES ('129', 'Laura', 'Bissot', 'LBISSOT', '650.124.5234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '3300.00', null, '121', '50');
INSERT INTO employees VALUES ('130', 'Mozhe', 'Atkinson', 'MATKINSO', '650.124.6234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2800.00', null, '121', '50');
INSERT INTO employees VALUES ('131', 'James', 'Marlow', 'JAMRLOW', '650.124.7234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2500.00', null, '121', '50');
INSERT INTO employees VALUES ('132', 'TJ', 'Olson', 'TJOLSON', '650.124.8234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2100.00', null, '121', '50');
INSERT INTO employees VALUES ('133', 'Jason', 'Mallin', 'JMALLIN', '650.127.1934',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '3300.00', null, '122', '50');
INSERT INTO employees VALUES ('134', 'Michael', 'Rogers', 'MROGERS', '650.127.1834',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2900.00', null, '122', '50');
INSERT INTO employees VALUES ('135', 'Ki', 'Gee', 'KGEE', '650.127.1734', to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2400.00', null, '122', '50');
INSERT INTO employees VALUES ('136', 'Hazel', 'Philtanker', 'HPHILTAN', '650.127.1634',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2200.00', null, '122', '50');
INSERT INTO employees VALUES ('137', 'Renske', 'Ladwig', 'RLADWIG', '650.121.1234',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '3600.00', null, '123', '50');
INSERT INTO employees VALUES ('138', 'Stephen', 'Stiles', 'SSTILES', '650.121.2034', to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '3200.00', null, '123', '50');
INSERT INTO employees VALUES ('139', 'John', 'Seo', 'JSEO', '650.121.2019',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2700.00', null, '123', '50');
INSERT INTO employees VALUES ('140', 'Joshua', 'Patel', 'JPATEL', '650.121.1834',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2500.00', null, '123', '50');
INSERT INTO employees VALUES ('141', 'Trenna', 'Rajs', 'TRAJS', '650.121.8009',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '3500.00', null, '124', '50');
INSERT INTO employees VALUES ('142', 'Curtis', 'Davies', 'CDAVIES', '650.121.2994', to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '3100.00', null, '124', '50');
INSERT INTO employees VALUES ('143', 'Randall', 'Matos', 'RMATOS', '650.121.2874',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2600.00', null, '124', '50');
INSERT INTO employees VALUES ('144', 'Peter', 'Vargas', 'PVARGAS', '650.121.2004',  to_date('2006-06-04','yyyy-mm-dd'), 'ST_CLERK', '2500.00', null, '124', '50');
INSERT INTO employees VALUES ('145', 'John', 'Russell', 'JRUSSEL', '011.44.1344.429268',  to_date('2006-06-04','yyyy-mm-dd'), 'SA_MAN', '14000.00', '0.40', '100', '80');
INSERT INTO employees VALUES ('146', 'Karen', 'Partners', 'KPARTNER', '011.44.1344.467268',  to_date('2006-06-04','yyyy-mm-dd'), 'SA_MAN', '13500.00', '0.30', '100', '80');
INSERT INTO employees VALUES ('147', 'Alberto', 'Errazuriz', 'AERRAZUR', '011.44.1344.429278',  to_date('2006-06-04','yyyy-mm-dd'), 'SA_MAN', '12000.00', '0.30', '100', '80');
INSERT INTO employees VALUES ('148', 'Gerald', 'Cambrault', 'GCAMBRAU', '011.44.1344.619268',  to_date('2006-06-04','yyyy-mm-dd'), 'SA_MAN', '11000.00', '0.30', '100', '80');
INSERT INTO employees VALUES ('207', '威廉', 'Gietz', 'WGIETZ', '515.123.8181', to_date('2006-06-04','yyyy-mm-dd'), 'AC_ACCOUNT', '8300.00', null, '205', '110');
INSERT INTO employees VALUES ('208', '古堡', '归归', 'WGIETZ', '515.123.8181', to_date('2006-06-04','yyyy-mm-dd'), 'AC_ACCOUNT', '8300.00', null, '205', '110');
INSERT INTO employees VALUES ('209', '中文名', 'Gietz', '中文呀', '515.123.8181', to_date('2006-06-04','yyyy-mm-dd'), 'AC_ACCOUNT', '8300.00', null, '205', '110');

###查询各个部门的平均工资，以及该部门的员工信息
SELECT 
	A.MANAGER_ID,A.EMPLOYEE_NAME,A.HIRE_DATE,A.SAL,B.AVG_SAL
FROM
	(SELECT MANAGER_ID,FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL FROM EMPLOYEES) A,
	(SELECT MANAGER_ID,AVG(SAL) AVG_SAL FROM EMPLOYEES GROUP BY MANAGER_ID) B
WHERE A.MANAGER_ID=B.MANAGER_ID
ORDER BY A.MANAGER_ID;

SELECT 
	A.MANAGER_ID,A.EMPLOYEE_NAME,A.HIRE_DATE,A.SAL,B.AVG_SAL
FROM
	(SELECT MANAGER_ID,FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL FROM EMPLOYEES) A,
	(SELECT MANAGER_ID,AVG(SAL) AVG_SAL FROM EMPLOYEES GROUP BY MANAGER_ID) B
WHERE A.MANAGER_ID=B.MANAGER_ID and A.EMPLOYEE_NAME = '威廉 Gietz'
ORDER BY A.MANAGER_ID;

SELECT 
	MANAGER_ID,FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME,HIRE_DATE,SAL,
	AVG(SAL) OVER (PARTITION BY MANAGER_ID) AVG_SAL
FROM EMPLOYEES;

SELECT 
	MANAGER_ID,FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME,HIRE_DATE,SAL,
	AVG(SAL) OVER (PARTITION BY MANAGER_ID) AVG_SAL
FROM EMPLOYEES where FIRST_NAME||' '||LAST_NAME like '威廉%';

###按照入职时间排序，并计算第一个员工到当前员工的平均工资（如果时间一样则相同时间一起计算）
SELECT 
	MANAGER_ID, FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL, 
	AVG(SAL) OVER (ORDER BY HIRE_DATE)
FROM EMPLOYEES where FIRST_NAME||' '||LAST_NAME like '威廉%';

###按照入职时间排序，且按照部门经理进行分组，并计算该部门第一个员工到当前员工的平均工资（如果时间一样则相同时间一起计算）
SELECT 
	MANAGER_ID, FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL, 
	AVG(SAL) OVER (PARTITION BY MANAGER_ID ORDER BY HIRE_DATE)
FROM EMPLOYEES where FIRST_NAME||' '||LAST_NAME like '威廉%';



###按照入职时间排序，且按照部门经理进行分组，计算当前员工的前一个到后两个共四个员工的平均工资（如果时间一样，则默认按照先后顺序计算）
SELECT 
	MANAGER_ID, FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL, 
	AVG(SAL) OVER (PARTITION BY MANAGER_ID ORDER BY HIRE_DATE ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)
FROM EMPLOYEES where FIRST_NAME||' '||LAST_NAME like '威廉%';

###按照入职时间排序，且按照部门经理分组，计算当前员工雇佣时间之前的50天以内，之后的150天以内之间的平均工资（未验证前50，后150的边界问题）
SELECT 
	MANAGER_ID, FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL,
	AVG(SAL) OVER (PARTITION BY MANAGER_ID ORDER BY HIRE_DATE RANGE BETWEEN 50 PRECEDING AND 150 FOLLOWING)
FROM EMPLOYEES where FIRST_NAME||' '||LAST_NAME like '威廉%';

###求平均做一个总结，并展示第一个到最后一个的取值方法
SELECT 
	MANAGER_ID, FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL,
	AVG(SAL) OVER (PARTITION BY MANAGER_ID ORDER BY HIRE_DATE) AVG_SAL_PART_ORDER,--累计求平均，和第3个一样
	AVG(SAL) OVER (PARTITION BY MANAGER_ID ) AVG_SAL_ORDER,--求整个部门的平均，和第1个一样
	AVG(SAL) OVER (PARTITION BY MANAGER_ID ORDER BY HIRE_DATE RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AVG_SAL_UNBOUND1, --求整个部门的平均，表示取值范围第一个到最后一个，结果和上面一致
	AVG(SAL) OVER (PARTITION BY MANAGER_ID ORDER BY HIRE_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AVG_SAL_UNBOUND2--求整个部门的平均，行号取值范围第一个到最后一个，结果和上面一致
FROM EMPLOYEES where FIRST_NAME||' '||LAST_NAME like '威廉%';


###MIN、MAX、AVG、SUM、COUNT
SELECT 
	DEPARTMENT_ID, FIRST_NAME||' '||LAST_NAME EMPLOYEE_NAME, HIRE_DATE, SAL,
	MIN(SAL) OVER (PARTITION BY DEPARTMENT_ID ORDER BY HIRE_DATE) AS DEPT_MIN,--按照部门ID分组，然后按照入职时间排序，计算包括当前员工入职时间及之前的所有员工的最低薪资（如果时间一样，则一起计算）
	MAX(SAL) OVER (PARTITION BY DEPARTMENT_ID ORDER BY HIRE_DATE) AS DEPT_MAX,--按照部门ID分组，然后按照入职时间排序，计算包括当前员工入职时间及之前的所有员工的最高薪资（如果时间一样，则一起计算）
	AVG(SAL) OVER (PARTITION BY DEPARTMENT_ID ORDER BY HIRE_DATE) AS DEPT_AVG,--按照部门ID分组，然后按照入职时间排序，计算该部门第一个员工到当前员工的平均工资（如果时间一样则相同时间一起计算）
	SUM(SAL) OVER (PARTITION BY DEPARTMENT_ID ORDER BY HIRE_DATE) AS DEPT_SUM,--按照部门ID分组，然后按照入职时间排序，计算该部门第一个员工到当前员工的工资和（如果时间一样则相同时间一起计算）
	COUNT(1) OVER (ORDER BY SAL) AS COUNT_BY_SAL,--按照工资进行排序，统计从第一个到当前工资的个数（如果工资一样，则一起统计）
	COUNT(1) OVER (ORDER BY SAL RANGE BETWEEN 50 PRECEDING AND 150 FOLLOWING) AS COUNT_BY_SAL_RANGE--按照工资进行排序，统计比当前工资小50到比当前工资大150的个数
FROM EMPLOYEES  where FIRST_NAME||' '||LAST_NAME like '威廉%';


--disable_warnings
--error 0,942
drop table EMPLOYEES;
--enable_warnings

###
--disable_warnings
--error 0,942
DROP TABLE EMP;
--error 0,942
DROP TABLE DEPT;
--enable_warnings

CREATE TABLE DEPT
(DEPTNO NUMBER(2),
	DNAME NVARCHAR2(20),
	LOC VARCHAR2(20) );
CREATE TABLE EMP
(EMPNO NUMBER(4),
	ENAME NVARCHAR2(20),
	JOB NVARCHAR2(20),
	MGR NUMBER(4),
	HIREDATE DATE,
	SAL NUMBER(7,2),
	COMM NUMBER(7,2),
	DEPTNO NUMBER(2));
INSERT INTO DEPT VALUES
	(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT VALUES (20,'RESEARCH','DALLAS');
INSERT INTO DEPT VALUES
	(30,'SALES','CHICAGO');
INSERT INTO DEPT VALUES
	(40,'OPERATIONS','BOSTON');
INSERT INTO DEPT VALUES
	(40,'中文测试001','BOSTON');
INSERT INTO DEPT VALUES
	(40,'中文测试001','BOSTON');
INSERT INTO DEPT VALUES
	(30,'中文测试002','中文测试002');
INSERT INTO DEPT VALUES
	(30,'中文测试001','中文测试001');

INSERT INTO EMP VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO EMP VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO EMP VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO EMP VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO EMP VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO EMP VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO EMP VALUES
(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO EMP VALUES
(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO EMP VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO EMP VALUES
(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20);
INSERT INTO EMP VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO EMP VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);
INSERT INTO EMP VALUES
(7955,'弥勒','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1315,NULL,30);
INSERT INTO EMP VALUES
(7966,'佛','BUYIYANG',7780,to_date('24-1-1982','dd-mm-yyyy'),1355,NULL,40);



###--按照deptno分组，然后计算当前行至最后一行的汇总
SELECT 
	EMPNO,ENAME,DEPTNO,SAL,
	SUM(SAL) OVER(PARTITION BY DEPTNO ORDER BY ENAME ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) MAX_SAL
FROM EMP where ENAME like '弥勒%';
 
 
###--按照deptno分组，然后计算当前行的上一行(rownum-1)到当前行的汇总
SELECT 
	EMPNO,ENAME,DEPTNO,SAL,
    SUM(SAL) OVER(PARTITION BY DEPTNO ORDER BY ENAME ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) MAX_SAL
FROM EMP where ENAME like '弥勒%';
 
 
###--按照deptno分组，然后计算当前行的上一行(rownum-1)到当前行的下两行(rownum+2)的汇总
SELECT 
	EMPNO,ENAME,DEPTNO,SAL,
    SUM(SAL) OVER(PARTITION BY DEPTNO ORDER BY ENAME ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) MAX_SAL
FROM EMP where ENAME like '弥勒%';
###--注意ROWS BETWEEN 1 preceding AND 1 following 是指当前行的上一行(rownum-1)到当前行的下两行(rownum+2)的汇总



SELECT ENAME,JOB,SAL ,LAG(SAL) OVER(ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';
 
SELECT ENAME,JOB,SAL ,LAG(SAL,1,500) OVER(ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';
 
SELECT ENAME,JOB,SAL ,LAG(SAL,2) OVER(ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';
 
SELECT ENAME,JOB,SAL ,LEAD(SAL) OVER(ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';
 
SELECT ENAME,JOB,SAL ,LEAD(SAL,1) OVER(ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';
 
SELECT ENAME,JOB,SAL ,LEAD(SAL,2) OVER(ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';
 
SELECT ENAME,JOB,SAL ,LEAD(SAL,1) OVER(PARTITION BY JOB ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';
 
SELECT ENAME,JOB,SAL ,LAG(SAL,1) OVER(PARTITION BY JOB ORDER BY SAL) LAST_SAL FROM EMP where ENAME like '弥勒%';

###--查询各部门工资最高的2名员工信息
SELECT * FROM (SELECT DEPTNO,ENAME,SAL,ROW_NUMBER() OVER (PARTITION BY DEPTNO ORDER BY SAL DESC) RN FROM EMP) WHERE RN<3 and  ENAME = '佛'; 

###--查询全公司工资最高的第6-10名员工
select * from (SELECT ENAME,SAL,ROW_NUMBER() OVER (ORDER BY SAL DESC) RN FROM EMP) where  RN>=6 and RN<=10;

###--查询大于本部门平均工资的员工信息
select a.DEPTNO,a.ENAME,a.SAL from EMP a,(select DEPTNO,avg(SAL) as SAL from EMP group by DEPTNO) b where a.DEPTNO=b.DEPTNO and a.SAL>b.SAL;

###--对排序的理解
###--sql无排序，over()排序子句省略
SELECT 
    DEPTNO,EMPNO,ENAME,SAL, 
    LAST_VALUE(SAL) OVER(PARTITION BY DEPTNO)
FROM EMP where ENAME = '弥勒';

###--sql无排序，over()排序子句有，窗口省略
SELECT 
    DEPTNO,EMPNO,ENAME,SAL,
    LAST_VALUE(SAL) OVER(PARTITION BY DEPTNO ORDER BY SAL DESC)
FROM EMP where ENAME = '弥勒';

###--sql无排序，over()排序子句有，窗口也有，窗口特意强调全组数据
SELECT 
    DEPTNO,EMPNO,ENAME,SAL,
    LAST_VALUE(SAL) OVER(PARTITION BY DEPTNO ORDER BY SAL ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) MAX_SAL
FROM EMP where ENAME = '弥勒';

###--sql有排序（正序），over()排序子句无，先做sql排序再进行分析函数运算
SELECT 
    DEPTNO,MGR,ENAME,SAL,HIREDATE,
    LAST_VALUE(SAL) OVER(PARTITION BY DEPTNO) LAST_VALUE
FROM EMP where ENAME = '弥勒' ORDER BY DEPTNO, SAL ;


###--sql有排序（倒序），over()排序子句有，窗口子句无，此时的运算是：sql先选数据但是不排序，而后排序子句先排序并进行分析函数处理（窗口默认为第一行到当前行），最后再进行sql排序
SELECT 
    DEPTNO,MGR,ENAME,SAL,HIREDATE,
    LAST_VALUE(SAL) OVER(PARTITION BY DEPTNO ORDER BY SAL ASC) LAST_VALUE
FROM EMP  where ENAME = '弥勒' ORDER BY DEPTNO,SAL DESC;


######
SELECT ENAME,JOB,MGR,COUNT(*) OVER() FROM EMP;
SELECT ENAME,JOB,MGR,COUNT(*) OVER(ORDER BY MGR) FROM EMP;
SELECT ENAME,JOB,MGR,COUNT(*) OVER(PARTITION BY JOB) FROM EMP;
SELECT ENAME,JOB,MGR,COUNT(*) OVER(PARTITION BY JOB ORDER BY MGR) FROM EMP;

select ename,JOB,MGR,sum(SAL) over() from emp;
select ename,JOB,MGR,sum(SAL) over(order by MGR) from emp;
select ename,JOB,MGR,sum(SAL) over(partition by JOB) from emp;
select ename,JOB,MGR,sum(SAL) over(partition by JOB order by MGR) from emp;

select ename,JOB,MGR,avg(SAL) over() from emp;
select ename,JOB,MGR,avg(SAL) over(order by MGR) from emp;
select ename,JOB,MGR,avg(SAL) over(partition by JOB) from emp;
select ename,JOB,MGR,avg(SAL) over(partition by JOB order by MGR) from emp  where ENAME = '弥勒';

select ename,JOB,MGR,SAL,min(SAL) over() from emp;
select ename,JOB,MGR,SAL,min(SAL) over(order by MGR) from emp;
select ename,JOB,MGR,SAL,min(SAL) over(partition by JOB) from emp;
select ename,JOB,MGR,SAL,min(SAL) over(partition by JOB order by MGR) from emp;

select ename,JOB,MGR,SAL,max(SAL) over() from emp;
select ename,JOB,MGR,SAL,max(SAL) over(order by MGR) from emp;
select ename,JOB,MGR,SAL,max(SAL) over(partition by JOB) from emp;
select ename,JOB,MGR,SAL,max(SAL) over(partition by JOB order by MGR) from emp;

select ename,MGR,rank() over(partition by job order by MGR) from emp;
select ename,MGR,rank() over(order by MGR) from emp;

select ename,MGR,dense_rank() over(partition by job order by MGR) from emp;
select ename,MGR,dense_rank() over(order by MGR) from emp;

select ename,MGR,row_number() over(partition by job order by MGR) from emp;
select ename,MGR,row_number() over(order by MGR) from emp;

select ename,SAL,ntile(3) over(order by SAL desc) from emp;
select ename,SAL,ntile(3) over(partition by job order by SAL desc) from emp;

select ename,first_value(SAL) over() from emp;
select ename,first_value(SAL) over(order by SAL desc) from emp;
select ename,first_value(SAL) over(partition by job) from emp;
select ename,first_value(SAL) over(partition by job order by SAL desc) from emp;
 
select ename,last_value(ename) over() from emp;
###select ename,last_value(ename) over(order by SAL desc) from emp;
###select ename,last_value(ename) over(partition by job) from emp;
select ename,last_value(ename) over(partition by job order by SAL desc) from emp;

###select 
###	job,max(SAL) keep(dense_rank first order by SAL desc),
###	max(SAL) keep(dense_rank last order by SAL desc) 
###from emp
###group by job;

select
	ename,MGR,lag(MGR,1,0) over(order by SAL),
	lead(MGR,1,0) over(order by SAL)
from emp;
 
select
	ename,MGR,lag(MGR,1) over(partition by JOB order by SAL),
	lead(MGR,1) over(partition by JOB order by SAL)
from emp;

select ename,job,SAL,ratio_to_report(1) over() from emp;
select ename,job,SAL,ratio_to_report(SAL) over() from emp;
select ename,job,SAL,ratio_to_report(1) over(partition by job) from emp;
select ename,job,SAL,ratio_to_report(SAL) over(partition by job) from emp;

select ename,job,SAL,percent_rank() over(order by SAL) from emp;
select ename,job,SAL,percent_rank() over(partition by job order by SAL) from emp;

select ename,job,SAL,cume_dist() over(order by SAL) from emp;
select ename,job,SAL,cume_dist() over(partition by job order by SAL) from emp;

--disable_warnings
--error 0,942
DROP TABLE EMP;
--error 0,942
DROP TABLE DEPT;
--enable_warnings

--disable_warnings
--error 0,942
DROP TABLE T;
--enable_warnings

create table T( 
   BILL_MONTH nvarchar2(12) , 
   AREA_CODE number, 
   NET_TYPE varchar(2), 
   LOCAL_FARE number 
);

insert into t values('200405',5761,'G', 7393344.04); 
insert into t values('200405',5761,'J', 5667089.85); 
insert into t values('200405',5762,'G', 6315075.96); 
insert into t values('200405',5762,'J', 6328716.15); 
insert into t values('200405',5763,'G', 8861742.59); 
insert into t values('200405',5763,'J', 7788036.32); 
insert into t values('200405',5764,'G', 6028670.45); 
insert into t values('200405',5764,'J', 6459121.49); 
insert into t values('200405',5765,'G', 13156065.77); 
insert into t values('200405',5765,'J', 11901671.70); 
insert into t values('200406',5761,'G', 7614587.96); 
insert into t values('200406',5761,'J', 5704343.05); 
insert into t values('200406',5762,'G', 6556992.60); 
insert into t values('200406',5762,'J', 6238068.05); 
insert into t values('200406',5763,'G', 9130055.46); 
insert into t values('200406',5763,'J', 7990460.25); 
insert into t values('200406',5764,'G', 6387706.01); 
insert into t values('200406',5764,'J', 6907481.66); 
insert into t values('200406',5765,'G', 13562968.81); 
insert into t values('200406',5765,'J', 12495492.50); 
insert into t values('200407',5761,'G', 7987050.65); 
insert into t values('200407',5761,'J', 5723215.28); 
insert into t values('200407',5762,'G', 6833096.68); 
insert into t values('200407',5762,'J', 6391201.44); 
insert into t values('200407',5763,'G', 9410815.91); 
insert into t values('200407',5763,'J', 8076677.41); 
insert into t values('200407',5764,'G', 6456433.23); 
insert into t values('200407',5764,'J', 6987660.53); 
insert into t values('200407',5765,'G', 14000101.20); 
insert into t values('200407',5765,'J', 12301780.20); 
insert into t values('200408',5761,'G', 8085170.84); 
insert into t values('200408',5761,'J', 6050611.37); 
insert into t values('200408',5762,'G', 6854584.22); 
insert into t values('200408',5762,'J', 6521884.50); 
insert into t values('200408',5763,'G', 9468707.65); 
insert into t values('200408',5763,'J', 8460049.43); 
insert into t values('200408',5764,'G', 6587559.23); 
insert into t values('200408',5764,'J', 7342135.86); 
insert into t values('200408',5765,'G', 14450586.63); 
insert into t values('200408',5765,'J', 12680052.38); 

###commit;

SELECT 
	BILL_MONTH,AREA_CODE,SUM(LOCAL_FARE) LOCAL_FARE, 
    FIRST_VALUE(AREA_CODE) OVER(PARTITION BY BILL_MONTH ORDER BY SUM(LOCAL_FARE) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FIRSTVAL, 
    LAST_VALUE(AREA_CODE) OVER(PARTITION BY BILL_MONTH ORDER BY SUM(LOCAL_FARE) DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) LASTVAL 
FROM T GROUP BY BILL_MONTH, AREA_CODE ORDER BY BILL_MONTH;

SELECT 
	AREA_CODE,BILL_MONTH,LOCAL_FARE,
    SUM(LOCAL_FARE) OVER(PARTITION BY AREA_CODE ORDER BY TO_NUMBER(BILL_MONTH) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) "3month_sum", 
    AVG(LOCAL_FARE) OVER(PARTITION BY AREA_CODE ORDER BY TO_NUMBER(BILL_MONTH) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) "3month_avg", 
    MAX(LOCAL_FARE) OVER(PARTITION BY AREA_CODE ORDER BY TO_NUMBER(BILL_MONTH) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) "3month_max", 
    MIN(LOCAL_FARE) OVER(PARTITION BY AREA_CODE ORDER BY TO_NUMBER(BILL_MONTH) RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) "3month_min" 
 FROM (
	SELECT T.AREA_CODE, T.BILL_MONTH, SUM(T.LOCAL_FARE) LOCAL_FARE FROM T GROUP BY T.AREA_CODE, T.BILL_MONTH);

SELECT 
	AREA_CODE,BILL_MONTH,LOCAL_FARE,
    SUM(LOCAL_FARE) OVER(PARTITION BY AREA_CODE ORDER BY BILL_MONTH ASC) "LAST_SUM_VALUE" 
FROM (
	SELECT T.AREA_CODE, T.BILL_MONTH, SUM(T.LOCAL_FARE) LOCAL_FARE FROM T GROUP BY T.AREA_CODE, T.BILL_MONTH) 
ORDER BY AREA_CODE, BILL_MONTH;

--disable_warnings
--error 0,942
DROP TABLE T;
--enable_warnings

#check约束补充验证

--disable_warnings
--error 0,942
DROP TABLE test29;
--error 0,942
DROP TABLE test30;
--error 0,942
DROP TABLE test31;
--enable_warnings

###检查只能是男或者女

create table test29(         
	id number primary key, 
    sex nvarchar2(2) check(sex in ('男','女'))  
 ); 
 
insert into test29 values (1,'男');
insert into test29 values (2,'女');
--error 2290
insert into test29 values (3,'哈');
insert into test29 values (4,'');

create table test30(         
     id number primary key,         
     sex varchar2(3) check(sex ='男' or sex='女')  
);    

insert into test30 values (1,'男');
insert into test30 values (2,'女');
--error 2290
insert into test30 values (3,'哈');
insert into test30 values (4,'');

create table test31(         
     id number primary key,         
     sex varchar2(3) 
   ); 

###BUG ID:
###alter table test31 add constraint chkk check (sex ='男' or sex='女');  
###alter table test31 add constraint chkk check (sex in('男','女'));

###insert into test31 values (1,'男');
###insert into test31 values (2,'女');
###--error 2290
###insert into test31 values (3,'哈');
###insert into test31 values (4,'');

--disable_warnings
--error 0,942
DROP TABLE test29;
--error 0,942
DROP TABLE test30;
--error 0,942
DROP TABLE test31;
--enable_warnings

###在一个范围中间
--disable_warnings
--error 0,942
DROP TABLE test32;
--error 0,942
DROP TABLE test33;
--error 0,942
DROP TABLE test34 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test32(       
      id number primary key,       
      age number check(age>0 and age<120)
   );
--error 2290
insert into test32 values (1,0);
--error 2290
insert into test32 values (2,120);
insert into test32 values (3,60);
insert into test32 values (4,1);


create table test33(       
       id number primary key,       
      age number check(age between 12 and 30)
   );
insert into test33 values (1,30);
--error 2290
insert into test33 values (5,31);
--error 2290
insert into test33 values (2,120);
insert into test33 values (3,20);
insert into test33 values (4,13);


create table test34(       
        id number primary key ,       
        age number
   );
###master上mysqltest工具问题，报错unkown error,手动测试正常，下同
###alter table test34 add constraint ch_test34 check(age>0 and age<120);

insert into test34 values (1,30);
###--error 2290
###insert into test34 values (2,120);
insert into test34 values (3,60);
insert into test34 values (4,31);

--disable_warnings
--error 0,942
DROP TABLE test32;
--error 0,942
DROP TABLE test33;
--error 0,942
DROP TABLE test34 CASCADE CONSTRAINTS purge;
--enable_warnings

###长度大于某个值

--disable_warnings
--error 0,942
DROP TABLE test35;
--error 0,942
DROP TABLE test36 CASCADE CONSTRAINTS purge;
--enable_warnings


create table test35(       
          id number primary key,       
          password nvarchar2(10) check(length(password)=6)
   );
insert into test35 values (1,'密码长度六位');
--error 2290
insert into test35 values (2,'密码长度五');
insert into test35 values (3,'密码长度WU');
--error 2290
insert into test35 values (4,'密码长度WUWEI');

SELECT * FROM test35;
create table test36(  
id number primary key ,     
password nvarchar2(20)
);
###alter table test36 add constraint check_test36 check(length(password)=6);
###insert into test36 values (1,'密码长度六位');
--disable_warnings
--error 0,942
DROP TABLE test35;
--error 0,942
DROP TABLE test36 CASCADE CONSTRAINTS purge;
--enable_warnings

###数大于某个值
--disable_warnings
--error 0,942
DROP TABLE test37;
--error 0,942
DROP TABLE test38 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test37(
id number(10)primary key,
no number(10) check(no>1)
);

insert into test37 values (1,2);
--error 2290
insert into test37 values (1,1);

create table test38(
id number(10) primary key,
no number(10)
);
###alter table test38 add constraint ch_test38 check(no>1);
###--error 2290
###insert into test38 values (1,1);

--disable_warnings
--error 0,942
DROP TABLE test37 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test38 CASCADE CONSTRAINTS purge;
--enable_warnings

###只能是8位字符,前两位是 0 ,3~4位为数字,第 5 位为"_"下划线,6~8位为字母
--disable_warnings
--error 0,942
DROP TABLE test39 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test40 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test39(
       id number(10) primary key,    
       password varchar2(20) check(REGEXP_LIKE(password,'^00[0-9][0-9][_][a-z,A-Z][a-z,A-Z][a-z,A-Z]$'))
   );
insert into test39 values (1,'0011_aaa');

create table test40(
id number(10) primary key ,    
password varchar2(20) check(REGEXP_LIKE(password,'^00[0-9][0-9][_][a-z,A-Z][a-z,A-Z][a-z,A-Z]$') and (length(password)=8)));

###alter table test40 modify password varchar2(10) check((password like '00[0-9][0-9][_][a-z,A-Z][a-z,A-Z][a-z,A-Z]%') and (length(password)>1));
insert into test40 values(1,'0012_abc');

--error 600,2290
insert into test40 values(1,'0012_好的');
--error 600,2290
insert into test40 values(2,'0012_好');
--error 2290
insert into test40 values(2,'0012_123');
--error 2290
insert into test40 values(3,'0012_@@@');

--disable_warnings
--error 0,942
DROP TABLE test39 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE test40 CASCADE CONSTRAINTS purge;
--enable_warnings

###电子邮箱要含有@符号check(字段 like ‘%@%’)
--disable_warnings
--error 0,942
DROP TABLE test41 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test41(
id number(10) primary key,
email varchar2(40) check (email like '%@%')
);
insert into test41 values(1,'12@126.com');
--error 2290
insert into test41 values(2,'12126.com');
insert into test41 values(3,'中文@.com');

--disable_warnings
--error 0,942
DROP TABLE test41 CASCADE CONSTRAINTS purge;
--enable_warnings

###SQL中用check约束一列的首字母为’s’check(字段 like ‘s%’)
--disable_warnings
--error 0,942
DROP TABLE test42  CASCADE CONSTRAINTS purge;
--enable_warnings

create table test42(
id number(10) primary key,  
name varchar2(10) check(name like 's%')
);
insert into test42 values(1,'sname');
--error 2290
insert into test42 values(2,'name');

--disable_warnings
--error 0,942
DROP TABLE test42  CASCADE CONSTRAINTS purge;
--enable_warnings

###检查约束前3位和后8位均为数字字符:check(字段 like ‘[0-9][0-9][0-9]%[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]’)

--disable_warnings
--error 0,942
DROP TABLE test43 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test43(
id number(10) primary key,  
no varchar2(20)check(REGEXP_LIKE(no, '^[0-9][0-9][0-9]%[0-9][0-9][0-9][0-9][0-9]$'))
);
--error 2290
insert into test43 values(1,'12345678');
--error 2290
insert into test43 values(2,'中文测试');
--error 2290
insert into test43 values(3,'123abcdef');

--disable_warnings
--error 0,942
DROP TABLE test43 CASCADE CONSTRAINTS purge;
--enable_warnings
 
###如何建立检查身份证的约束,身份证是18位,最后一位还有可能是X
--disable_warnings
--error 0,942
DROP TABLE test44 CASCADE CONSTRAINTS purge;
--enable_warnings


create table test44(
id number(10) primary key,
no nvarchar2(18) check( length(no)=18)
);
insert into test44 values (1,'12345678912345678x');

###select 身份证号 from 表名 where len(身份证号) = 18 and (right(身份证号,17) like  '[0-9]'or right(身份证号,17) like 'x');
select no 身份证号 from test44 where length(no) = 18;

--disable_warnings
--error 0,942
DROP TABLE test44 CASCADE CONSTRAINTS purge;
--enable_warnings

###如何设置区号由0-9之间的数字组成CONSTRAINT

###quhao CHECK (quhao  LIKE '[0-9][0-9][0-9]' 
###or quhao LIKE '[0-9][0-9][0-9][0-9]'
###or quhao LIKE '[0-9][0-9][0-9][0-9][0-9]'));

--disable_warnings
--error 0,942
DROP TABLE test45 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test45(
id number(10) primary key,
quhao nvarchar2(18) check(REGEXP_LIKE(quhao,'^中文'))
);

--error 2290
insert into test45 values (1,'070');
insert into test45 values (2,'中文测试');

--disable_warnings
--error 0,942
DROP TABLE test45 CASCADE CONSTRAINTS purge;
--enable_warnings
###定义前八位为数字或者 -一共是15位，为CHAR型
--disable_warnings
--error 0,942
DROP TABLE test46 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test46(
id number(10) primary key,
ziduan nvarchar2(18)
);

###alter table test46 add constraint chk check(REGEXP_LIKE(ziduan,'^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'));
###alter table test46 add constraint chklen check(length(ziduan)=15);

insert into test46 values (1,'12345678abcdefg');

--disable_warnings
--error 0,942
DROP TABLE test46 CASCADE CONSTRAINTS purge;
--enable_warnings

###如何限制varchar字段不允许出现单引号的检查约束 ！！！
###设表为TALBENAME,不能有引号的字段为FIELDNAME 则

--disable_warnings
--error 0,942
DROP TABLE test47 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test47(
id number(10) primary key,
ziduan nvarchar2(18)
);

###ALTER TABLE test47 ADD CONSTRAINT CK_fieldname CHECK (not ziduan like '%''%');
###--error 2290
###insert into test47 values (1,'12345678''abcdefg');

insert into test47 values (1,'12345678abcdefg');

--disable_warnings
--error 0,942
DROP TABLE test47 CASCADE CONSTRAINTS purge;
--enable_warnings

###在表中的某列中通过检查约束，让其有某些固定的值

--disable_warnings
--error 0,942
DROP TABLE test48 CASCADE CONSTRAINTS purge;
--enable_warnings

create table test48(
id number(10) primary key,
ziduan nvarchar2(18) check(ziduan like 'bd[0-9][0-9][0-9][0-9][0-9][0-9]')
);

--error 2290
insert into test48 values (1,'bd345678');

--disable_warnings
--error 0,942
DROP TABLE test48 CASCADE CONSTRAINTS purge;
--enable_warnings

--disable_warnings
--error 0,942
DROP TABLE EMP CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE DEPT CASCADE CONSTRAINTS purge;
--enable_warnings


--echo  ##场景测试gb18030-6//其他特别关注点gb18030-6-2//gb18030-6-2-2
--disable_warnings
--error 0,942
drop table testtable;
--enable_warnings

####ORACLE关键字
CREATE TABLE testtable (c1 char(20),c2 nchar(20),c3 varchar2(20),c4 nvarchar2(20),c5 clob);
insert into testtable VALUES('DISTINCT','DISTINCT','DISTINCT','DISTINCT','DISTINCT');
insert into testtable VALUES('GROUP','GROUP','GROUP','GROUP','GROUP');
insert into testtable VALUES('UNIQUE','UNIQUE','UNIQUE','UNIQUE','UNIQUE');
insert into testtable VALUES('HAVING','HAVING','HAVING','HAVING','HAVING');
insert into testtable VALUES('&','&','&','&','&');
insert into testtable VALUES('!','!','!','!','!');
insert into testtable VALUES('true','true','true','true','true');
insert into testtable VALUES('TRUE','TRUE','TRUE','TRUE','TRUE');
insert into testtable VALUES('FALSE','FALSE','FALSE','FALSE','FALSE');
###31483229- 【Sqlancer】插入数据为关键字时，使用plan cache有报错
###insert into testtable VALUES(true,true,true,true,true);
###insert into testtable VALUES(TRUE,TRUE,TRUE,TRUE,TRUE);
###insert into testtable VALUES(FALSE,FALSE,FALSE,FALSE,FALSE);
###INSERT INTO testtable VALUES(true,true,true,true,true);
###INSERT INTO testtable VALUES(TRUE,TRUE,TRUE,TRUE,TRUE);
###INSERT INTO testtable VALUES(FALSE,FALSE,FALSE,FALSE,FALSE);
###INSERT INTO testtable VALUES(DISTINCT,GROUP,UNIQUE,HAVING,&);

select * from testtable;
select length(c1),length(c1),length(c1),length(c1),length(c1) from testtable;
select lengthb(c1),lengthb(c1),lengthb(c1),lengthb(c1),lengthb(c1) from testtable;
select upper(c1),upper(c1),upper(c1),upper(c1),upper(c1) from testtable;
select lower(c1),lower(c1),lower(c1),lower(c1),lower(c1) from testtable;
select nvl(c1,c2),nvl(c1,c3),nvl(c1,c4),nvl(c1,c5),nvl(c1,c5) from testtable;


###--echo  ##场景测试gb18030-6//其他特别关注点gb18030-6-2//gb18030-6-2-3;外层通过不同用例展示
###--echo  ##场景测试gb18030-6//其他特别关注点gb18030-6-2//gb18030-6-2-5；已有gb18030-1-1-1-1 等DDL/DML覆盖
###--echo  ##场景测试gb18030-6//其他特别关注点gb18030-6-2//gb18030-6-2-6：当前GB18030链路下，UTF8租户时，报错信息有中文均有乱码31554186- 【GB18030链路】报错信息中有中文时显示乱码+AS 列名为中文乱码

--disable_warnings
--error 0,00942
drop table gbk_dml_examp;
--error 0,00942
drop table gbk_dml_2;
--enable_warnings
create table gbk_dml_examp(c1 varchar2(4000),c2 int);
create table gbk_dml_2(c0 varchar2(4000),c1 varchar2(4000),c2 char(2000),c3 clob);
insert into gbk_dml_examp values('先帝创业未半而中道崩殂，今天下三分，益州疲弊，此诚危急存亡之秋也。然侍卫之臣不懈于内，忠志之士忘身于外者，盖追先帝之殊遇，欲报之于陛下也。诚宜开张圣听，以光先帝遗德，恢弘志士之气，不宜妄自菲薄，引喻失义，以塞忠谏之路也。
宫中府中，俱为一体，陟罚臧否，不宜异同。若有作奸犯科及为忠善者，宜付有司论其刑赏，以昭陛下平明之理，不宜偏私，使内外异法也。
侍中、侍郎郭攸之、费祎、董允等，此皆良实，志虑忠纯，是以先帝简拔以遗陛下。愚以为宫中之事，事无大小，悉以咨之，然后施行，必能裨补阙漏，有所广益。
将军向宠，性行淑均，晓畅军事，试用于昔日，先帝称之曰能，是以众议举宠为督。愚以为营中之事，悉以咨之，必能使行阵和睦，优劣得所。
亲贤臣，远小人，此先汉所以兴隆也；亲小人，远贤臣，此后汉所以倾颓也。先帝在时，每与臣论此事，未尝不叹息痛恨于桓、灵也。侍中、尚书、长史、参军，此悉贞良死节之臣，愿陛下亲之信之，则汉室之隆，可计日而待也。
臣本布衣，躬耕于南阳，苟全性命于乱世，不求闻达于诸侯。先帝不以臣卑鄙，猥自枉屈，三顾臣于草庐之中，咨臣以当世之事，由是感激，遂许先帝以驱驰。后值倾覆，受任于败军之际，奉命于危难之间，尔来二十有一年矣。
先帝知臣谨慎，故临崩寄臣以大事也。受命以来，夙夜忧叹，恐托付不效，以伤先帝之明，故五月渡泸，深入不毛。今南方已定，兵甲已足，当奖率三军，北定中原，庶竭驽钝，攘除奸凶，兴复汉室，还于旧都。此臣所以报先帝而忠陛下之职分也。至于斟酌损益，进尽忠言，则攸之、祎、允之任也。
愿陛下托臣以讨贼兴复之效，不效，则治臣之罪，以告先帝之灵。若无兴德之言，则责攸之、祎、允等之慢，以彰其咎；陛下亦宜自谋，以咨诹善道，察纳雅言，深追先帝遗诏，臣不胜受恩感激。
今当远离，临表涕零，不知所言。;',1);
insert into gbk_dml_2 select c1,c1,substr(c1,0,500),c1 from gbk_dml_examp;
###这里的length长度为750，lengthb长度为2232，则实际需要截取更短的才行
###select length(substr(c1,0,2000)) from gbk_dml_examp;
###select lengthb(substr(c1,0,2000)) from gbk_dml_examp;

select c0,c1,c2 from gbk_dml_2;
select count(1) from gbk_dml_2;

select length(c0),length(c1),length(c2),length(c3),lengthb(c0),lengthb(c1),lengthb(c2),lengthb(to_char(c3)) from gbk_dml_2;
select substr(c0,instr(c0,'自',1,2)-1,4),substr(c1,instr(c1,'自',1,2)-1,4),substr(c2,instr(c2,'自',1,2)-1,4) from gbk_dml_2;
--echo  # sql中验证函数的使用//cast  类型的转换
select cast(substr(c0,instr(c0,'自',1,2)-1,4) as varchar2(5)) from gbk_dml_2;
select cast(substr(c1,instr(c1,'自',1,2)-1,4) as char(4)) from gbk_dml_2;
select length(nvl('',c0)),length(c1),length(c2),length(c3),lengthb(c0),lengthb(c1),lengthb(c2) from gbk_dml_2;
select c0,c1,c2 from gbk_dml_2 where c1 like '%亲%臣%';
select ascii(c1) from gbk_dml_2;
insert into gbk_dml_examp values('<?xml version="1.0" encoding="UTF-8"?>
<students>
    <class a_name = "1606D">
        <student>
            <name>俄罗斯</name>
            <age>18</age>
            <sex>女</sex>
        </student>
        <student>
            <name>张三</name>
            <age>20</age>
            <sex>男</sex>
        </student>
        <student>
            <name>李四</name>
            <age>33</age>
            <sex>不知道</sex>
        </student>
    </class>
    <class a_name = "1606X">
        <student>
            <name>刘依男</name>
            <age>18</age>
            <sex>女</sex>
        </student>
        <student>
            <name>刘浩</name>
            <age>20</age>
            <sex>男</sex>
        </student>
        <student>
            <name>孙宏恩</name>
            <age>33</age>
            <sex>不知道</sex>
        </student>
    </class>
</students>
————————————————
版权声明：本文为CSDN博主「IOS技术小牛」的原创文章，遵循 CC 4.0 BY-SA 版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/weixin_42925415/article/details/81584477',2);

insert into gbk_dml_examp values('The Unanimous Declaration of the Thirteen United States of America . When, in the course of human events, it becomes necessary for one people to dissolve the political bonds which have connected them with another, and to assume among the powers of the earth, the separate and equal station to which the laws of nature and of nature\s God entitle them, a decent respect to the opinions of mankind requires that they should declare the causes which impel them to the separation
We hold these truths to be self-evident, that all men are created equal, that they are endowed by their Creator with certain unalienable rights, that among these are life, liberty and the pursuit of happiness. That to secure these rights, governments are instituted among men, deriving their just powers from the consent of the governed. That whenever any form of government becomes destructive to these ends, it is the right of the people to alter or to abolish it, and to institute new government, laying its foundation on such principles and organizing its powers in such form, as to them shall seem most likely to effect their safety and happiness.
He has refused his assent to laws, the most wholesome and necessary for the public good.
He has refused to pass other laws for the accommodation of large districts of people, unless those people would relinquish the right of representation in the legislature, a right inestimable to them and formidable to tyrants only.
He has called together legislative bodies at places unusual,uncomfortable, and distant from the depository of their public records, for the sole purpose of fatiguing them into compliance with his measures.
He has dissolved representative houses repeatedly, for opposing with manly firmness his invasions on the rights of the people.
He has refused for a long time, after such dissolutions, to cause others to be elected whereby the legislative powers,incapable of annihilation, have returned to the people at large for their exercise the state remaining in the meantime exposed to all the dangers of invasion from without, and convulsions within.
He has endeavored to prevent the population of these statesfor that purpose obstructing the laws for naturalization of foreigners refusing to pass others to encourage their migration hither, and raising the conditions of new appropriations of lands.',3);
insert into gbk_dml_examp values('曝料：明年iPhone要剪刘海儿；俞敏洪：当你工资比同学少一半，证明生命已经浪费一半……晚间回顾一下有哪些不容错过的科技新闻和网友的神回复。
卢伟冰回怼余承东：外国车牌贴手机上多卖一万是啥实用价值
9月26日，华为在国内发布了Mate 30系列产品，提到刚发布的小米5G环绕屏MIX Aphla概念机时，---@余承东 评价称只是概念手机，没有实用价值。对此，小米---@卢伟冰 发微博表示，如果说环绕屏没有“实用价值”，
那么把一个外国车牌贴在手机上就多卖1万块的“实用价值”是什么？
---@下午茶食乜好：有一说一，确实。
---@i猪喊你：嘴上说着没有实用价值，转过头抄华为施华洛世奇手机壳，499一个。
---@歪闷4：看成卢本伟了。
俞敏洪：当你工资比同学少一半，证明生命已经浪费一半
近日，俞敏洪在“中国大学生自强之星”分享会上表示，钱是一个人的能力证明，当你工资比同学少一半时，你的生命已经浪费了一半。每个人都只有24个小时，但有的人能把时间不断累加让自己变成一种更加自由的状态。
---@SmilEdo：为他们公关掬一把同情的泪。
---@围观群众张先生：你的资产不到马云的十分之一，你已经浪费了绝大部分的生命。
---@汤米饭215：科学家含泪跪拜企业家。
曝料：明年iPhone要剪刘海儿？还能保留面部识别技术？
---@BenGeskin 推特曝料称，苹果已设计了一款没有“刘海儿”的6.7英寸屏幕iPhone，用于人脸识别的原深感镜头等部件将被缩小，安置在显示屏上方的边
中。Geskin做了一张示意图来说明所谓未来款iPhone，他说苹果计划在2020年发布这产品。',4);
insert into gbk_dml_examp values('',0);
insert into gbk_dml_2 select c1,c1,substr(c1,0,500),c1 from gbk_dml_examp;
select length(c0),lengthb(c0),length(c1),lengthb(c1),length(c2),lengthb(c2),length(c3) from gbk_dml_2 order by c1;


--disable_warnings
--error 0,00942
drop table gbk_dml_examp;
--error 0,00942
drop table gbk_dml_2;
--enable_warnings

--disable_warnings
--error 0,00942
drop table gbk_dml_一;
--enable_warnings

###create table gbk_dml_一(c0 varchar2(2),c1 varchar2(2),c2 char(2),列3 int)partition by hash(c1)subpartition by range(列3)subpartition template(subpartition 分区1 values less than(0),subpartition 分區二 values less than(500),subpartition こんにちは values less than (maxvalue));

##OK
##create table gbk_dml_一(c0 varchar2(2),c1 varchar2(2),c2 char(2),c3 int)partition by hash(c1)subpartition by range(c3)subpartition template(subpartition part1 values less than(0),subpartition part2 values less than(500),subpartition part3 values less than (maxvalue));

##NG
#create table gbk_dml_一(c0 varchar2(2),c1 varchar2(2),c2 char(2),列3 int)partition by hash(c1)subpartition by range(列3)subpartition template(subpartition part1 values less than(0),subpartition part2 values less than(500),subpartition part3 values less than (maxvalue));

##OK
create table gbk_dml_一(c0 varchar2(10),c1 varchar2(10),c2 char(10),c3 int)partition by hash(c1)subpartition by range(c3)subpartition template(subpartition 分区1 values less than(0),subpartition 分區二 values less than(500),subpartition こんにちは values less than (maxvalue));

##create table gbk_dml_一(c0 varchar2(10),c1 varchar2(10),c2 char(10),列3 int)partition by hash(列3)subpartition by range(列3)subpartition template(subpartition part1 values less than(0),subpartition part2 values less than(500),subpartition part3 values less than (maxvalue));


insert into gbk_dml_一 values('1 ','1 ','1 ',1);
insert into gbk_dml_一 values('10','10','10',2);
insert into gbk_dml_一 values('Ⅻ','Ⅻ','Ⅻ',3);
insert into gbk_dml_一 values('字','字','字',4);
insert into gbk_dml_一 values('體','體','體',5);
insert into gbk_dml_一 values('〇','〇','〇',6);
insert into gbk_dml_一 values('龠','龠','龠',7);
insert into gbk_dml_一 values('㈥','㈥','㈥',9);
insert into gbk_dml_一 values('','','',10);

select * from gbk_dml_一;
select c3 as 中文列测试,c0,c1,c2,lengthb(c0) nchar_c0_字节长度,lengthb(c1) nchar_c1_字节长度,length(c2) char_c2_字节长度,length(c3) char_c3_字节长度,length(c0) 列0長度,length(c1) 列1長度,length(c2) 列2長度,length(c3) 列3長度 from gbk_dml_一 order by 中文列测试;

--disable_warnings
--error 0,00942
drop table gbk_dml_一;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_nchar_at;
--enable_warnings
create table gbk_nchar_at(c1 varchar2(20));
insert into gbk_nchar_at values('adsfadsf
\@aasdf');
select * from gbk_nchar_at;
--disable_warnings
--error 0,00942
drop table gbk_nchar_at;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22774513;
--enable_warnings
create table gbk_22774513(c1 varchar2(2))partition by hash(c1);
--error 14060
alter table gbk_22774513 modify c1 char(2);
--error 0,00942
drop table gbk_22774513;

###--disable_warnings
###--error 0,00942
###drop table gbk_22774393;
###--enable_warnings
###create table gbk_22774393(c1 char(2));
###--error 00600  暂不支持
###alter table gbk_22774393 modify c1 varchar2(100);

--disable_warnings
--error 0,00942
drop table gbk_insert_part4016_bug;
--enable_warnings
create table gbk_insert_part4016_bug(c0 varchar2(2),c1 varchar2(2),c2 char(2),列3 int)partition by hash(c1);
insert into gbk_insert_part4016_bug values('','??','',8);
select * from gbk_insert_part4016_bug;


--disable_warnings
--error 0,00942
drop table gbk_22786385;
--enable_warnings
create table gbk_22786385(c1 varchar2(100));
insert into gbk_22786385 values('你seesee你');
--error 0,900
select convert(c1,'ZHS16GBK','UTF8') from gbk_22786385;
--error 0,900
select convert('你seesee你','ZHS16GBK','UTF8') from dual;
select ascii('字!@#$%^&*()_+龠') from dual;


--disable_warnings
--error 0,00942
drop table gbk_22825346;
--enable_warnings
--error 0,600
create table gbk_22825346(col1 number(10) primary key,name varchar2(10) not null,sex varchar2(10),pep_no_1 number(10) unique,city varchar2(40) default '杭州',remark varchar2(25),check(sex='男' or sex='女'));
--disable_warnings
--error 0,00942
drop table gbk_22825346;
--enable_warnings

#转义问题，暂无规划
#--disable_warnings
#--error 0,00942
#drop table gbk_22831357 ;
#--enable_warnings
#create table gbk_22831357(c1 varchar2(20));
#insert into gbk_22831357 values('&字符测试');
#select * from gbk_22831357;

--disable_warnings
--error 0,00942
drop table gbk_22831783;
--enable_warnings
create table gbk_22831783(c1 varchar2(50),字段2  number);
insert into gbk_22831783 values('士大夫',67);
insert into gbk_22831783 values('安\德森',63);
insert into gbk_22831783 values('201303第三方的',77);
insert into gbk_22831783 values('杜少府',68);
insert into gbk_22831783 values('公环銸境',97);
insert into gbk_22831783 values('试点乛范围',62);
insert into gbk_22831783 values('繁體字測試',87);
select cast(c1 as varchar2(2)) as empno from gbk_22831783;
select cast(c1 as varchar2(3)) as empno from gbk_22831783;


--disable_warnings
--error 0,00942,00903
drop table "双引号 命名 测试 ";
--enable_warnings
--error 00904
create table "双引号 命名 测试 "("列 1" varchar2(20)," 繁 體 " varchar2(20));

--disable_warnings
--error 0,00942
drop table gbk_22852103;
--enable_warnings
create table gbk_22852103(c1 varchar2(10),c2 varchar2(10)) partition by hash(c1)subpartition by list(c2)subpartition template(subpartition p1 values('分区1'),subpartition p2 values('分区2'))partitions 20;
--disable_warnings
--error 0,00942
drop table gbk_22852103;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22852568;
--enable_warnings
create table gbk_22852568(c1 int,c2 varchar2(20))partition by hash(c1)subpartition by range(c2)subpartition template(subpartition p1 values less than (8),subpartition p2 values less than (maxvalue))partitions 5;
--disable_warnings
--error 0,00942
drop table gbk_22852568;
--enable_warnings

--disable_warnings
--error 0,00942
drop table gbk_22854471;
--enable_warnings
create table gbk_22854471(c1 int,c2 varchar2(20),c3 varchar2(60) generated always as (concat(c1,c2))) partition by hash(c3)subpartition by range(c1)subpartition template(subpartition p1 values less than (8),subpartition p2 values less than (maxvalue))partitions 20;
--disable_warnings
--error 0,00942
drop table gbk_22854471;
--enable_warnings

--disable_warnings
--error 0,00942
drop table gbk_22856902;
--error 0,00942
drop table gbk_22856902_1;
--error 0,00942
drop table gbk_22856902_2;
--enable_warnings
create table gbk_22856902(c1 varchar2(4000));
insert into gbk_22856902 values('先帝创业未半而中道崩殂，今天下三分，益州疲弊，此诚危急存亡之秋也。然侍卫之臣不懈于内，忠志之士忘身于外者，盖追先帝之殊遇，欲报之于陛下也。诚宜开张圣听，以光先帝遗德，恢弘志士之气，不宜妄自菲薄，引喻失义，以塞忠谏之路也。
宫中府中，俱为一体，陟罚臧否，不宜异同。若有作奸犯科及为忠善者，宜付有司论其刑赏，以昭陛下平明之理，不宜偏私，使内外异法也。
侍中、侍郎郭攸之、费祎、董允等，此皆良实，志虑忠纯，是以先帝简拔以遗陛下。愚以为宫中之事，事无大小，悉以咨之，然后施行，必能裨补阙漏，有所广益。
将军向宠，性行淑均，晓畅军事，试用于昔日，先帝称之曰能，是以众议举宠为督。愚以为营中之事，悉以咨之，必能使行阵和睦，优劣得所。
亲贤臣，远小人，此先汉所以兴隆也；亲小人，远贤臣，此后汉所以倾颓也。先帝在时，每与臣论此事，未尝不叹息痛恨于桓、灵也。侍中、尚书、长史、参军，此悉贞良死节之臣，愿陛下亲之信之，则汉室之隆，可计日而待也。
臣本布衣，躬耕于南阳，苟全性命于乱世，不求闻达于诸侯。先帝不以臣卑鄙，猥自枉屈，三顾臣于草庐之中，咨臣以当世之事，由是感激，遂许先帝以驱驰。后值倾覆，受任于败军之际，奉命于危难之间，尔来二十有一年矣。
先帝知臣谨慎，故临崩寄臣以大事也。受命以来，夙夜忧叹，恐托付不效，以伤先帝之明，故五月渡泸，深入不毛。今南方已定，兵甲已足，当奖率三军，北定中原，庶竭驽钝，攘除奸凶，兴复汉室，还于旧都。此臣所以报先帝而忠陛下之职分也。至于斟酌损益，进尽忠言，则攸之、祎、允之任也。
愿陛下托臣以讨贼兴复之效，不效，则治臣之罪，以告先帝之灵。若无兴德之言，则责攸之、祎、允等之慢，以彰其咎；陛下亦宜自谋，以咨诹善道，察纳雅言，深追先帝遗诏，臣不胜受恩感激。
今当远离，临表涕零，不知所言。;');
create table gbk_22856902_1(c1 int,c2 varchar2(200),c3 timestamp,c4 date,c5 char(200),c6 varchar2(200),c7 number(6,2),c8 varchar2(400) generated always as (concat(c1,c2))) partition by hash(c2)subpartition by range(c1)subpartition template(subpartition p1 values less than (8),subpartition p2 values less than (maxvalue))partitions 20;
--echo 创建关联表
create table gbk_22856902_2(c1 int,c2 varchar2(200),c3 timestamp,c4 date,c5 char(200),c6 varchar2(200),c7 number(6,2),c8 varchar2(400) generated always as (concat(c1,c2))) partition by hash(c6)subpartition by range(c1)subpartition template(subpartition p1 values less than (8),subpartition p2 values less than (maxvalue))partitions 20;

delimiter //;
begin 
for i in reverse 0..30 loop 
insert into gbk_22856902_1(c1,c2,c3,c4,c5,c6,c7) 
select i,substr(c1,i*10,10),to_date('201909','yyyymm'),to_date('201910','yyyymm'),substr(c1,i*10+1,10),substr(c1,i*10+2,10),i*4+i/7 from gbk_22856902; 
end loop;
end;
//


begin 
for i in reverse 0..30 loop 
insert into gbk_22856902_2(c1,c2,c3,c4,c5,c6,c7) 
select i,substr(c1,i*20,10),to_date('201910','yyyymm'),to_date('201909','yyyymm'),substr(c1,i*20+1,10),substr(c1,i*20+2,10),i*4+i/7 from gbk_22856902; 
end loop;
end;
//
delimiter ;//
select * from (select a.c1 as c1,a.c2 as c2 from gbk_22856902_1 a join gbk_22856902_2 b on a.c2=b.c2 and a.c5=b.c5 and a.c6=b.c6 order by c1 desc) minus select * from (select a.c1 as c1,a.c2 as c2 from gbk_22856902_1 a join gbk_22856902_2 b on a.c2=b.c2 and a.c5=b.c5 and a.c6=b.c6 order by c1);

--disable_warnings
--error 0,00942
drop table gbk_22856902;
--error 0,00942
drop table gbk_22856902_1;
--error 0,00942
drop table gbk_22856902_2;
--enable_warnings



--disable_warnings
--error 0,00942
drop table gbk_22864636;
--enable_warnings
create table gbk_22864636(c1 int,c2 int,c3 timestamp,c4 timestamp with time zone,c5 timestamp with local time zone,c6 date,c7 char(5),c8 varchar2(10),c9 number(6,2),c10 int generated always as (c1+c2)) partition by hash(c1)subpartition by range(c2)subpartition template(subpartition p1 values less than (4),subpartition p2 values less than (maxvalue))partitions 20;
insert into gbk_22864636(c1,c2) values (9999999999999999999999999999999999999,9999999999999999999999999999999999999);
insert into gbk_22864636(c1,c2) select c1/10,c2*10/10-65535 from gbk_22864636 where c1>10;
delete from gbk_22864636 where c1=1000000000000000000000000000000000000;
insert into gbk_22864636(c1,c2,c3,c4,c5,c6,c7,c8,c9) select to_number(c7)+to_number(c8) as c1,to_number(c7)/to_number(c8)+c1 as c2,to_date('201908','yyyymm')+6789 c3,c3 c4,c4+2,c3-22,substr(c1,0,4),substr(c2,0,instr(c2,'9',1,1)),(c2-c1)/666 from gbk_22864636 where c1>10;
insert into gbk_22864636(c1,c2,c3,c4,c5,c6,c7,c8,c9) select to_number(c7)+to_number(c8) as c1,to_number(c7)/to_number(c8)+c1 as c2,to_date('201908','yyyymm')+6789 c3,c3 c4,c4+2,c3-22,substr(c1,0,4),substr(c2,0,instr(c2,'9',1,1)),(c2-c1)/666 from gbk_22864636 where c8='9';
--disable_warnings
--error 0,00942
drop table gbk_22864636;
--enable_warnings

--echo bug_id:22865074
--disable_warnings
--error 0,00942
drop table gbk_22865074;
--enable_warnings
create table gbk_22865074(c1 varchar2(1));
insert into gbk_22865074 values(' ');
select lengthb(c1) from gbk_22865074;


--disable_warnings
--error 0,00942
drop table gbk_22869971;
--enable_warnings
create table gbk_22869971(c0 varchar2(5),c1 varchar2(5),c2 char(5),c3 int);
insert into gbk_22869971 values('','','',1);
insert into gbk_22869971 values('¨','¨','¨',2);
insert into gbk_22869971 values('÷','÷','÷',3);
insert into gbk_22869971 values('哈','哈','哈',4);
select c0||'|'||c1||'|'||c2||'|'||c3 from gbk_22869971 order by c3;
--disable_warnings
--error 0,00942
drop table gbk_22869971;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22870453;
--enable_warnings
create table gbk_22870453(c0 varchar2(50),c1 varchar2(50),c2 char(50),c4 varchar2(50),c5 varchar2(50),c3 int);
insert into gbk_22870453 values('','','','','',1);
insert into gbk_22870453 values('¨','¨','¨','¨','¨',2);
insert into gbk_22870453 values('÷','÷','÷','÷','÷',3);
select c0|| '|' ||c1|| '|' ||c2|| '|' ||c3|| '|' ||lengthb(c0)|| '|' ||lengthb(c1)|| '|' ||lengthb(c2)|| '|' ||lengthb(c3)|| '|' ||lengthb(c4)|| '|' ||lengthb(c5)|| '|' ||length(c0)|| '|' ||length(c1)|| '|' ||length(c2)|| '|' ||length(c3)|| '|' ||length(c4)|| '|' ||length(c5) from gbk_22870453 order by c3;

--disable_warnings
--error 0,00942
drop table gbk_22870453;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22873282;
--enable_warnings
create table gbk_22873282(c1 varchar2(50));
insert into gbk_22873282 values('杜少府');
insert into gbk_22873282 values('繁體字');
insert into gbk_22873282 values('asd');
insert into gbk_22873282 values('ADS');
select cast(c1 as varchar2(2)) as empno from gbk_22873282;
--disable_warnings
--error 0,00942
drop table gbk_22873282;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22878124;
--enable_warnings
create table gbk_22878124(c1 int,c2 char(20),c8 varchar2(60) generated always as (concat(c1,c2)));
--disable_warnings
--error 0,00942
drop table gbk_22878124;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22878347;
--error 0,00942
drop table gbk_22878347_1;
--enable_warnings
create table gbk_22878347_1(c1 int,c2 char(200),c3 timestamp,c4 date,c5 varchar2(200),c6 varchar2(200),c7 number(6,2),c8 varchar2(400) generated always as (concat(c1,c2)));
create table gbk_22878347(c1 char(2000));
insert into gbk_22878347 values('先帝创业未半而中道崩殂，今天下三分，益州疲弊，此诚危急存亡之秋也。然侍卫之臣不懈于内，忠志之士忘身于外者，盖追先帝之殊遇，欲报之于陛下也。诚宜开张圣听，以光先帝遗德，恢弘志士之气，不宜妄自菲薄，引喻失义，以塞忠谏之路也。');
insert into gbk_22878347_1(c1,c2,c3,c4,c5,c6,c7) select 1,substr(c1,1*20,10),to_date('201910','yyyymm'),to_date('201909','yyyymm'),substr(c1,1*20+1,10),substr(c1,1*20+2,10),1*4+1/7 from gbk_22878347;
--disable_warnings
--error 0,00942
drop table gbk_22878347;
--error 0,00942
drop table gbk_22878347_1;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22926153_1;
--error 0,00942
drop table gbk_22926153_2;
--enable_warnings
create table gbk_22926153_2(c1 varchar2(2),c2 number(2));
create table gbk_22926153_1(c1 varchar2(2),c2 number(2));
insert into gbk_22926153_1 values('a',7);
delimiter /;
declare
v1 char(2);
v2 varchar2(2);
v3 varchar2(2);
aaa int;
nums int;
begin
select count(*) into nums from gbk_22926153_1;
for i in reverse 1..nums loop
select c1 into v1 from gbk_22926153_1 where c2=7;
select c1 into v2 from gbk_22926153_1 where c2=7;
select c1 into v3 from gbk_22926153_1 where c2=7;
insert into gbk_22926153_2 values(v1,1);
insert into gbk_22926153_2 values(v2,1+20);
insert into gbk_22926153_2 values(v3,1+40);
end loop;
end;
/
delimiter ;/
select * from gbk_22926153_2 order by c1,c2;

--disable_warnings
--error 0,00942
drop table gbk_22926153_1;
--error 0,00942
drop table gbk_22926153_2;
--enable_warnings



--echo bug_id:22949346
--disable_warnings
--error 0,00942
drop table gbk_22949346;
--error 0,00942
drop table gbk_22949346_1;
--error 0,04080
drop trigger gbk_trigger_22949346;
--enable_warnings
create table gbk_22949346(c1 varchar2(1000));
insert into gbk_22949346 values('曝料：明年iPhone要剪刘海儿；俞敏洪：当你工资比同学少一半，证明生命已经浪费一半……晚间回顾一下有哪些不容错过的科技新闻和网友的神回复。
卢伟冰回怼余承东：外国车牌贴手机上多卖一万是啥实用价值
9月26日，华为在国内发布了Mate 30系列产品，提到刚发布的小米5G环绕屏MIX Aphla概念机时，---@余承东 评价称只是概念手机，没有实用价值。对此，小米---@卢伟冰 发微博表示，如果说环绕屏没有“实用价值”，
那么把一个外国车牌贴在手机上就多卖1万块的“实用价值”是什么？
---@下午茶食乜好：有一说一，确实。
---@i猪喊你：嘴上说着没有实用价值，转过头抄华为施华洛世奇手机壳，499一个。
---@歪闷4：看成卢本伟了。');
create table gbk_22949346_1(c1 varchar2(200));
delimiter /;
create or replace trigger gbk_trigger_22949346 before insert on gbk_22949346_1 for each row declare aaa int; begin
if :new.c1 is null or :new.c1='-' then
select '占位' into :new.c1 from dual;
end if;
end gbk_trigger_22949346;
/
declare
nums int;
insert_value char(10);
begin
select length(c1) into nums from gbk_22949346;
nums := nums-1;
for i in reverse 0..(nums) loop
select substr(c1,i,1) into insert_value from gbk_22949346;
insert into gbk_22949346_1 values(insert_value);
end loop;
end;
/
delimiter ;/
--disable_warnings
--error 0,00942
drop table gbk_22949346;
--error 0,00942
drop table gbk_22949346_1;
--error 0,04080
drop trigger gbk_trigger_22949346;
--enable_warnings


--disable_warnings
--error 0,00942
drop table gbk_22954287;
--error 0,00942
drop table gbk_22954287_1;
--error 0,04080
drop trigger gbk_trigger_22954287;
--enable_warnings
create table gbk_22954287(c1 varchar2(1000));
insert into gbk_22954287 values('曝料：明年iPhone要剪刘海儿；俞敏洪：当你工资比同学少一半，证明生命已经浪费一半……晚间回顾一下有哪些不容错过的科技新闻和网友的神回复。
卢伟冰回怼余承东：外国车牌贴手机上多卖一万是啥实用价值
9月26日，华为在国内发布了Mate 30系列产品，提到刚发布的小米5G环绕屏MIX Aphla概念机时，---@余承东 评价称只是概念手机，没有实用价值。对此，小米---@卢伟冰 发微博表示，如果说环绕屏没有“实用价值”，
那么把一个外国车牌贴在手机上就多卖1万块的“实用价值”是什么？
---@下午茶食乜好：有一说一，确实。
---@i猪喊你：嘴上说着没有实用价值，转过头抄华为施华洛世奇手机壳，499一个。
---@歪闷4：看成卢本伟了。');
create table gbk_22954287_1(c1 varchar2(200),c2 int);
###delimiter /;
###BUG：
###create or replace trigger gbk_trigger_22954287 before insert on gbk_22954287_1 for each row declare aaa int; begin
###if :new.c1 is null or :new.c1='-' then
###select '占位' into :new.c1 from dual;
###end if;
###end gbk_trigger_22954287;
###/
###delimiter ;/
###insert into gbk_22954287_1 values('-',500);
###insert into gbk_22954287_1 values('',400);
###insert into gbk_22954287_1 values('-',500);
###select * from gbk_22954287_1 order by c1,c2;

select cast(rpad('哈哈',20,'加') as varchar2(40)),length(cast(rpad('哈哈',20,'加') as varchar2(40))) from dual;

--disable_warnings
--error 0,00942
drop table gbk_22954287;
--error 0,00942
drop table gbk_22954287_1;
--error 0,04080
drop trigger gbk_trigger_22954287;
--enable_warnings


--echo bug_id:22978299
--disable_warnings
--error 0,00942
drop table gbk_22978299;
--error 0,00600
drop procedure procedure_22978299;
--enable_warnings
create table gbk_22978299(c1 char(20),c2 varchar2(20),c3 varchar2(20),c4 clob,c5 number(1),c6 date);
insert into gbk_22978299 values('哈','哈','哈','哈',1,to_date('20191015164024','yyyymmddhh24miss'));
delimiter /;
create or replace procedure procedure_22978299 is
begin
 declare
   type c is ref cursor;    
   table_sor c;      
   v_c1 gbk_22978299.c1%type;     
   v_c2 gbk_22978299.c2%type;
   v_c3 gbk_22978299.c3%type;
   v_c4 gbk_22978299.c4%type;
   v_c5 gbk_22978299.c5%type;
   v_c6 gbk_22978299.c6%type;
   aaa int;
   bbb varchar2(100);
begin
  open table_sor for select c1,c2,c3,c4,c5,c6 from gbk_22978299 order by c5;       
  loop        
    fetch table_sor into v_c1,v_c2,v_c3,v_c4,v_c5,v_c6;
   select v_c1||chr(45)||length(v_c1)||chr(45)||lengthb(v_c1) into bbb from dual;
    exit when table_sor%notfound;        
   end loop;         
   close table_sor;     
end;
end procedure_22978299;
/
delimiter ;/
call procedure_22978299();
select * from gbk_22978299;

--disable_warnings
--error 0,00942
drop table gbk_22978299;
--error 0,00600
drop procedure procedure_22978299;
--enable_warnings

--disable_warnings
--error 0,00942
drop table gbk_pl11;
--error 0,00942
drop table gbk_pl12;
--enable_warnings
create table gbk_pl12(c1 varchar2(1000));
insert into gbk_pl12 values('曝料：明年iPhone要剪刘海儿；俞敏洪：当你工资比同学少一半，证明生命已经浪费一半……晚间回顾一下有哪些不容错过的科技新闻和网友的神回复。
卢伟冰回怼余承东：外国车牌贴手机上多卖一万是啥实用价值
9月26日，华为在国内发布了Mate 30系列产品，提到刚发布的小米5G环绕屏MIX Aphla概念机时，---@余承东 评价称只是概念手机，没有实用价值。对此，小米---@卢伟冰 发微博表示，如果说环绕屏没有“实用价值”，
那么把一个外国车牌贴在手机上就多卖1万块的“实用价值”是什么？
---@下午茶食乜好：有一说一，确实。
---@i猪喊你：嘴上说着没有实用价值，转过头抄华为施华洛世奇手机壳，499一个。
---@歪闷4：看成卢本伟了。');
create table gbk_pl11(c1 varchar2(20),c2 int);
delimiter /;
declare
nums int;
insert_value char(20);
begin
select length(c1) into nums from gbk_pl12;
nums := nums-1;
for i in reverse 0..(nums) loop 
select substr(c1,i,1) into insert_value from gbk_pl12;
insert into gbk_pl11 values(insert_value,i); 
end loop;
end;
/
delimiter ;/
insert into gbk_pl11 values('',400);
select * from gbk_pl11 where c2=400;
insert into gbk_pl11 values('-',500);
select * from gbk_pl11 where c2=500;

--disable_warnings
--error 0,04080
drop trigger gbk_trigger1;
--error 0,00942
drop table gbk_pl11;
--error 0,00942
drop table gbk_pl12;
--enable_warnings


###hash分区
--disable_warnings
--error 0,942
DROP TABLE DEPT;
--error 0,942
DROP TABLE EMP;
--enable_warnings

CREATE TABLE DEPT
(DEPTNO NUMBER(2) PRIMARY KEY,
	DNAME NVARCHAR2(14),
	LOC VARCHAR2(13) ) ;
CREATE TABLE EMP
       (EMPNO NUMBER(4) PRIMARY KEY,
	ENAME NVARCHAR2(10),
	JOB NVARCHAR2(9),
	MGR NUMBER(4),
	HIREDATE DATE,
	SAL NUMBER(7,2),
	COMM NUMBER(7,2),
	DEPTNO NUMBER(2));
INSERT INTO DEPT VALUES
	(10,'ACCOUNTING','NEW YORK');
INSERT INTO DEPT VALUES (20,'RESEARCH','DALLAS');
INSERT INTO DEPT VALUES
	(30,'SALES','CHICAGO');
INSERT INTO DEPT VALUES
	(40,'OPERATIONS','BOSTON');
INSERT INTO EMP VALUES
(7369,'SMITH','CLERK',7902,to_date('17-12-1980','dd-mm-yyyy'),800,NULL,20);
INSERT INTO EMP VALUES
(7499,'ALLEN','SALESMAN',7698,to_date('20-2-1981','dd-mm-yyyy'),1600,300,30);
INSERT INTO EMP VALUES
(7521,'WARD','SALESMAN',7698,to_date('22-2-1981','dd-mm-yyyy'),1250,500,30);
INSERT INTO EMP VALUES
(7566,'JONES','MANAGER',7839,to_date('2-4-1981','dd-mm-yyyy'),2975,NULL,20);
INSERT INTO EMP VALUES
(7654,'MARTIN','SALESMAN',7698,to_date('28-9-1981','dd-mm-yyyy'),1250,1400,30);
INSERT INTO EMP VALUES
(7698,'BLAKE','MANAGER',7839,to_date('1-5-1981','dd-mm-yyyy'),2850,NULL,30);
INSERT INTO EMP VALUES
(7782,'CLARK','MANAGER',7839,to_date('9-6-1981','dd-mm-yyyy'),2450,NULL,10);
INSERT INTO EMP VALUES
(7788,'SCOTT','ANALYST',7566,to_date('13-4-1987','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7839,'KING','PRESIDENT',NULL,to_date('17-11-1981','dd-mm-yyyy'),5000,NULL,10);
INSERT INTO EMP VALUES
(7844,'TURNER','SALESMAN',7698,to_date('8-9-1981','dd-mm-yyyy'),1500,0,30);
INSERT INTO EMP VALUES
(7876,'ADAMS','CLERK',7788,to_date('13-5-1987', 'dd-mm-yyyy'),1100,NULL,20);
INSERT INTO EMP VALUES
(7900,'JAMES','CLERK',7698,to_date('3-12-1981','dd-mm-yyyy'),950,NULL,30);
INSERT INTO EMP VALUES
(7902,'FORD','ANALYST',7566,to_date('3-12-1981','dd-mm-yyyy'),3000,NULL,20);
INSERT INTO EMP VALUES
(7934,'MILLER','CLERK',7782,to_date('23-1-1982','dd-mm-yyyy'),1300,NULL,10);
INSERT INTO EMP VALUES
(7955,'弥勒','收银员',7780,to_date('23-1-1982','dd-mm-yyyy'),1355,NULL,20);
INSERT INTO EMP VALUES
(7966,'','职业',7781,to_date('23-1-1982','dd-mm-yyyy'),1455,NULL,20);
INSERT INTO EMP VALUES
(7977,'姓名','',7781,to_date('23-1-1982','dd-mm-yyyy'),1555,NULL,20);


####测试多个函数嵌套
select EMPNO, ENAME, nvl(to_char(JOB), '收银员') from EMP where nvl(to_char(JOB), '收银员') = '收银员';

select ENAME,nvl(to_char(decode(ENAME,'','林海燕','弥勒','陈德财','老王')),'老王') as name from EMP where nvl(to_char(decode(ENAME,'','林海燕','弥勒','陈德财','老王')),'老王') = '老王';

####测试复杂sql语句
select e.job,count(e.job) from emp e where e.HIREDATE >= to_date('1975-10-01', 'yyyy-mm-dd')
and ((e.ename = '姓名' and e.EMPNO != '07') or
e.job != '收银员')
and exists (select * from emp e where e.HIREDATE >= to_date('1975-10-01', 'yyyy-mm-dd') and e.empno > 0 and e.DEPTNO >0)
and not exists (select * from dept t where t.DEPTNO >= 40 and t.DEPTNO = e.DEPTNO) and e.job = '职业'
group by e.job order by e.job;


--disable_warnings
--error 0,942
DROP TABLE DEPT;
--error 0,942
DROP TABLE EMP;
--enable_warnings



###表情符,ORACLE本身不支持
--disable_warnings
--error 0,942
DROP TABLE testgbk;
--error 0,942
DROP TABLE testgbk2;
--enable_warnings

create table testgbk(num int , bq1 nvarchar2(100) primary key,bq2 varchar2(100),bq3 nchar(200),bq4 char(200));
insert into testgbk values(1,'~@^_^@~','~@^_^@~','~@^_^@~','~@^_^@~');
insert into testgbk values(2,'%>_<%','%>_<%','%>_<%','%>_<%');
insert into testgbk values(3,'..@_@|||||..','..@_@|||||..','..@_@|||||..','..@_@|||||..');
insert into testgbk values(4,'\xF0\x9F\x92\x94','\xF0\x9F\x92\x94','\xF0\x9F\x92\x94','\xF0\x9F\x92\x94');
insert into testgbk values(5,'0xE2 0x98 0x80','0xE2 0x98 0x80','0xE2 0x98 0x80','0xE2 0x98 0x80');
insert into testgbk values(6,'0x2600','0x2600','0x2600','0x2600');
insert into testgbk values(7,'0xD83D 0xDC2F','0xD83D 0xDC2F','0xD83D 0xDC2F','0xD83D 0xDC2F');
select * from testgbk t where t.bq1= '~@^_^@~' and t.bq2 = '~@^_^@~' and t.bq2 = '~@^_^@~' and t.bq2 = '~@^_^@~';
select * from testgbk t where t.bq1= '0xD83D 0xDC2F' and t.bq2 = '0xD83D 0xDC2F' and t.bq2 = '0xD83D 0xDC2F' and t.bq2 = '0xD83D 0xDC2F';

create table testgbk2 as select * from testgbk;

select * from testgbk2 t where t.bq1= '~@^_^@~' and t.bq2 = '~@^_^@~' and t.bq2 = '~@^_^@~' and t.bq2 = '~@^_^@~';
select * from testgbk2 t where t.bq1= '0xD83D 0xDC2F' and t.bq2 = '0xD83D 0xDC2F' and t.bq2 = '0xD83D 0xDC2F' and t.bq2 = '0xD83D 0xDC2F';

--disable_warnings
--error 0,942
DROP TABLE testgbk;
--error 0,942
DROP TABLE testgbk2;
--enable_warnings


####使用Connect by进行递归查询
--disable_warnings
--error 0,942
DROP TABLE BBSTEST;
--enable_warnings

CREATE TABLE BBSTEST
(
PARENTID INTEGER NOT NULL,
ID INTEGER NOT NULL,
NAME NVARCHAR2(200) NOT NULL
);
insert into BBSTEST (PARENTID,ID,NAME) values (0,1,'数据库开发');
insert into BBSTEST (PARENTID,ID,NAME) values (1,11,'DB2');
insert into BBSTEST (PARENTID,ID,NAME) values (11,111,'DB2 文章1');
insert into BBSTEST (PARENTID,ID,NAME) values (111,1111,'DB2 文章1 的评论1');
insert into BBSTEST (PARENTID,ID,NAME) values (111,1112,'DB2 文章1 的评论2');
insert into BBSTEST (PARENTID,ID,NAME) values (11,112,'DB2 文章2');
insert into BBSTEST (PARENTID,ID,NAME) values (1,12,'Oracle');
insert into BBSTEST (PARENTID,ID,NAME) values (0,2,'Java 技术');
COMMIT;
  
select * from BBSTEST
start with name = 'DB2 文章1 的评论2'
connect by prior PARENTID = ID;

INSERT INTO BBSTEST(PARENTID,ID,NAME)VALUES(111,1,'DB2论文');  
  
###select * from BBSTEST    
###start with name = 'DB2 文章1 的评论2'
###connect by prior PARENTID = ID;
###NOCYCLE

select a.ID,
       a.PARENTID,
       level "层次",
       sys_connect_by_path(ID, '->') "合并层次",
       prior a.PARENTID "父节点",
       connect_by_root a.ID "根节点",
       decode(connect_by_isleaf, 1, a.ID, null) "子节点",
       decode(connect_by_isleaf, 1, '是', '否') "是否子节点"
from BBSTEST a
start with name = 'DB2 文章1 的评论2'
connect by NOCYCLE prior PARENTID = ID;


--disable_warnings
--error 0,942
DROP TABLE BBSTEST;
--enable_warnings



####转换类函数
--disable_warnings
--error 0,942
DROP TABLE CCCTEST;
--enable_warnings


CREATE TABLE CCCTEST
(
PARENTID INTEGER NOT NULL,
ID INTEGER NOT NULL,
NAME NVARCHAR2(200) NOT NULL,
JOB VARCHAR2(200) NOT NULL,
NCHARCOL NCHAR(200) NOT NULL,
CHARCOL CHAR(200) NOT NULL
);

insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (0,1,'数据库开发','数据库开发','数据库开发','数据库开发');
insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (1,11,'DB2','DB2','DB2','DB2');
insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (11,111,'DB2 文章1','DB2 文章1','DB2 文章1','DB2 文章1');
insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (111,1111,'DB2 文章1 的评论1','DB2 文章1 的评论1','DB2 文章1 的评论1','DB2 文章1 的评论1');
insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (111,1112,'DB2 文章1 的评论2','DB2 文章1 的评论2','DB2 文章1 的评论2','DB2 文章1 的评论2');
insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (11,112,'DB2 文章2','DB2 文章2','DB2 文章2','DB2 文章2');
insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (1,12,'Oracle','Oracle','Oracle','Oracle');
insert into CCCTEST (PARENTID,ID,NAME,JOB,NCHARCOL,CHARCOL) values (0,2,'Java 技术','Java 技术','Java 技术','Java 技术');

--error 01410,600
select chartorowid(t.NAME) from CCCTEST t;
--error 01410,600
select chartorowid(t.NAME) from CCCTEST t where t.ID = 12;
--error 01410,600
select chartorowid(t.JOB) from CCCTEST t;
--error 01410,600
select chartorowid(t.JOB) from CCCTEST t where t.ID = 12;
--error 01410,600
select chartorowid(t.NCHARCOL) from CCCTEST t;
--error 01410,600
select chartorowid(t.CHARCOL) from CCCTEST t;

###select rowid,rowidtochar(rowid) from emp;
###select rowid,rowidtochar(rowid) from emp where emp.rowid = 'AAAm6BAAHAAAAkEAAA';
###select rowid,rowidtochar(rowid) from emp where emp.rowid = '*AAEPAQEAwMkcAAA=';


###1.OB报错01410，ORACLE正常
###SELECT chartorowid('AAAADeAABAAAAZSAAA') FROM DUAL;
###select rowid,chartorowid('ssssffffgggghhhhrr'),ename from emp;
###SELECT chartorowid('*AAEPAQEAwMkcAAA=') FROM DUAL;

--error 01410,600
SELECT chartorowid('中文字符串测试模板中文字符串测试模板') FROM DUAL;


###2.OB报错00904，ORACLE正常
###SELECT rowid FROM DUAL;
###SELECT rowid,ROWIDTOCHAR(rowid) FROM DUAL;
###SELECT rowid,ROWIDTOCHAR(ROWID) FROM DUAL;
###show create dual;

###SELECT ROWIDTOCHAR('AAAADeAABAAAAZSAAA') FROM DUAL;
###SELECT ROWIDTOCHAR('*AAEPAQEAwMkcAAA=') FROM DUAL;
###SELECT ROWIDTOCHAR('中文字符串测试模板中文字符串测试模板') FROM DUAL;
###desc dual;
###select owner, object_name , object_type from dba_objects where object_name like '%DUAL%';

--disable_warnings
--error 0,942
DROP TABLE CCCTEST;
--enable_warnings


###to_char
###select sysdate as 默认格式日期,to_char(sysdate,'YYYY-MM-DD') as 转换后的日期 from dual;

###select to_char(to_date('2002-08-26','yyyy-mm-dd'),'day',
###'NLS_DATE_LANGUAGE = American') from dual; 
###select * from dual where to_char(to_date('2002-08-26','yyyy-mm-dd'),'day',
###'NLS_DATE_LANGUAGE = American') = 'monday'; 

--disable_warnings
--error 0,942
DROP TABLE DDDTEST;
--enable_warnings

CREATE TABLE DDDTEST
(
ID INTEGER NOT NULL,
NAME NVARCHAR2(200) NOT NULL,
JOB VARCHAR2(200) NOT NULL,
NCHARCOL NCHAR(200) NOT NULL,
CHARCOL CHAR(200) NOT NULL
);
insert into DDDTEST (ID,NAME,JOB,NCHARCOL,CHARCOL) values (1,'2020-08-11 15:50:34','2020-08-11 15:50:34','2020-08-11 15:50:34','2020-08-11 15:50:34');
insert into DDDTEST (ID,NAME,JOB,NCHARCOL,CHARCOL) values (2,'中文测试001','中文测试001','中文测试001','中文测试001');
insert into DDDTEST (ID,NAME,JOB,NCHARCOL,CHARCOL) values (3,'123456.78','123456.78','123456.78','123456.78');

--error 1481,01722
select to_char(t.NAME,'yyyy-mm-dd hh24:mi:ss') as nowTime from DDDTEST t;
--error 1481,01722
select to_char(t.JOB,'yyyy-mm-dd hh24:mi:ss') as nowTime from DDDTEST t;
--error 1481,01722
select to_char(t.NCHARCOL,'yyyy-mm-dd hh24:mi:ss') as nowTime from DDDTEST t;
###--error 1481,01722
###select to_char(t.CHARCOL,'yyyy-mm-dd hh24:mi:ss') as nowTime from DDDTEST t;

###select to_date(t.NAME,'yyyy-mm-dd hh24:mi:ss') from DDDTEST t;
###select to_date(t.JOB,'yyyy-mm-dd hh24:mi:ss') from DDDTEST t;
###select to_date(t.NCHARCOL,'yyyy-mm-dd hh24:mi:ss') from DDDTEST t;
###select to_date(t.CHARCOL,'yyyy-mm-dd hh24:mi:ss') from DDDTEST t;
###
###select * from DDDTEST t where to_date(t.NAME,'yyyy-mm-dd hh24:mi:ss') = '2020-08-11 15:50:34';


####TO_NUMBER验证
select TO_NUMBER(t.NAME) from DDDTEST t where t.ID = 3;
select TO_NUMBER(t.JOB) from DDDTEST t where t.ID = 3;
select TO_NUMBER(t.NCHARCOL) from DDDTEST t where t.ID = 3;
select TO_NUMBER(t.CHARCOL) from DDDTEST t where t.ID = 3;

SELECT TO_NUMBER('123.45') FROM dual;
SELECT TO_NUMBER('$123,456.78','$999,999.99') FROM dual;
select to_number('00001228') from dual;
select trunc(to_number('123.123'),2) from dual;

###OB暂不支持
###SELECT TO_NUMBER('0A', 'XX') FROM dual;
###SELECT TO_NUMBER(100000,'XXXXXXXX') FROM dual;
###select to_number('0123') number1,
###trunc(to_number('0123.123'),2) number2,
###to_number('120.11','999.99') number3,
###to_number('0a','xx') number4,
###to_number(100000,'xxxxxx') number5 
###from dual;


####OB有报错，ORACLE正常
###UPDATE emp SET SAL = SAL + 
###   TO_NUMBER('100.00', '9G999D99')
###   WHERE ENAME = '弥勒';

--disable_warnings
--error 0,942
DROP TABLE DDDTEST;
--enable_warnings


--echo  ########pivot test scene##########

--disable_warnings
--error 0,942
drop table sales;
--enable_warnings

create table sales
(
  product     NVARCHAR2(32) not null,
  country  VARCHAR2(10),
  channel   NVARCHAR2(100),
  quarter  CHAR(2),
  amount_sold NVARCHAR2(32),
  quantity_sold varchar2(32)
);

insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('牛肉面','中国','3','01','7','100');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('牛肉面','中国','3','02','7','150');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('牛肉面','中国','4','02','7','200');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('牛肉面','中国','5','03','7','300');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('牛肉面','中国','9','04','7','400');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('肯德基','美国','3','01','20','100');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('肯德基','美国','4','02','20','200');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('肯德基','美国','5','03','20','300');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('肯德基','美国','9','04','20','400');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('披萨','日本','3','01','15','100');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('披萨','日本','4','02','15','200');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('披萨','日本','5','03','15','300');
insert into sales (product,country,channel,quarter,amount_sold,quantity_sold)values('披萨','日本','9','04','15','400');

#单列

SELECT *
  FROM (SELECT product, channel, amount_sold FROM sales) 
PIVOT(SUM(amount_sold)
   FOR CHANNEL IN(3 AS 直营店,
                  4 AS 电商,
                  5 AS 自媒体,
                  9 AS 加盟店))
 ORDER BY product;
 
select * from (SELECT *
  FROM (SELECT product, channel, amount_sold FROM sales) 
PIVOT(SUM(amount_sold)
   FOR CHANNEL IN(3 AS 直营店,
                  4 AS 电商,
                  5 AS 自媒体,
                  9 AS 加盟店))
 ORDER BY product) t where t.直营店 = '15';
 
 

#多列
##SELECT * FROM (SELECT product, channel, quarter, quantity_sold FROM sales) PIVOT(SUM(quantity_sold) FOR(channel, quarter) 
##IN((5, '03') AS 自媒体三季度销售量,
##   (4, '02') AS 电商二季度销售量,
##   (3, '01') AS 直营店一季度销售量,
##   (3, '02') AS 直营店二季度销售量,
##   (9, '04') AS 加盟店四季度销售量));
   
select * from (SELECT * FROM (SELECT product, channel, quarter, quantity_sold FROM sales) PIVOT(SUM(quantity_sold) FOR(channel, quarter) 
IN((5, '03') AS 自媒体三季度销售量,
   (4, '02') AS 电商二季度销售量,
   (3, '01') AS 直营店一季度销售量,
   (3, '02') AS 直营店二季度销售量,
   (9, '04') AS 加盟店四季度销售量))) t where t.PRODUCT = '牛肉面';

  

#多个聚合

SELECT *
  FROM (SELECT product, channel, amount_sold, quantity_sold FROM sales)
PIVOT(SUM(amount_sold) AS sums, SUM(quantity_sold) AS sumq
   FOR channel IN(5, 4, 3, 9))
 ORDER BY product;


--disable_warnings
--error 0,942
drop table t1 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t_utf16 CASCADE CONSTRAINTS purge;
--error 0,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings


--echo ###insert test###
create table t_utf16(c1 nchar(200) not null unique, c2 nchar(200) not null, c3 nchar(200) default '0FF' not null, c4 generated always as(c2 || c3), c5 nvarchar2(200) unique,  c6 nchar(200) unique, c7 nvarchar2(200) not null, c8 nchar(200) default '0FF' not null, c9 nvarchar2(500) generated always as(c7 || c8),c10 nchar(200) unique, c11 nvarchar2(200) not null, c12 nchar(200) default '0FF' not null, c13 nvarchar2(500) generated always as(c11 || c12),constraint pks1_c6 primary key(c1,c6,c10), constraint fk_s1 FOREIGN KEY(c3,c8,c12) REFERENCES t_utf16(c1,c6,c10), constraint c1_check check((c2 || c7 || c11) not in ('0A0A0A','0B0B0B','0C0C0C','0D0D0D','0E0E0E'))) pctfree 3  pctused 3 initrans 3  enable row movement PARTITION BY hash(c1) SUBPARTITION BY range(c1) SUBPARTITION TEMPLATE (SUBPARTITION  t_utf16_p0 VALUES LESS THAN ('1aaaaaaaaaaaaa'),SUBPARTITION  t_utf16_p1 VALUES LESS THAN ('2aaaaaaaaaaaaa11'),SUBPARTITION  t_utf16_p2 VALUES LESS THAN ('3aaaaaaaaaaaaa1122'),SUBPARTITION  t_utf16_p3 VALUES LESS THAN ('4aaaaaaaaaaaaa112244'),SUBPARTITION  t_utf16_p4 VALUES LESS THAN ('5aaaaaaaaaaaaa11224455'),SUBPARTITION  t_utf16_pp VALUES LESS THAN (MAXVALUE)) PARTITIONS 10;

insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('1234','1','1234','1234','2234','2','2234','3234','3','3234');
insert into t_utf16 (c1,c2,c3,c5,c6,c7,c8,c10,c11,c12) values('中文测试123','中文测试123','中文测试123','中文测试123','中文测试123','中文测试123','中文测试123','中文测试123','中文测试123','中文测试123');
###select * from t_utf16;
--error 0,942
DROP TABLE t_utf16 CASCADE CONSTRAINTS purge;

--disable_warnings
--error 0,942
drop table t1 CASCADE CONSTRAINTS purge;
--error 0,942
DROP TABLE t_utf16 CASCADE CONSTRAINTS purge;
--error 0,942
drop table tt_utf16 CASCADE CONSTRAINTS purge;
--enable_warnings

###--disable_warnings
###--error 0,942
###drop table test CASCADE CONSTRAINTS purge;
###--error 0,942
###drop table testtype CASCADE CONSTRAINTS purge;
###--error 0,600,4043
###drop type t_air;
###--error 0,600,4043
###drop type t_air force;
###--error 0,600,4043
###drop type nvarchar2_100_object;
###--enable_warnings
###
###create type t_air  as object(id int ,name varchar(20));
###/
###create table test(id int ,person t_air);
###insert into test values(1,t_air(1,'23sdf'));
###
###create or replace type nvarchar2_100_object as object(o1 nvarchar2(100),o2 nvarchar2(100),o3 nvarchar2(100),o4 nvarchar2(100),o5 nvarchar2(100),o6 nvarchar2(100),o7 nvarchar2(100),o8 nvarchar2(100),o9 nvarchar2(100),o10 nvarchar2(100));
###/
###create table testtype(id int ,person nvarchar2_100_object);
###insert into test values(1,nvarchar2_100_object('中文测试001','中文测试001','中文测试001','中文测试001','中文测试001','中文测试001','中文测试001','中文测试001','中文测试001','中文测试001'));
###
###--disable_warnings
###--error 0,942
###drop table test CASCADE CONSTRAINTS purge;
###--error 0,942
###drop table testtype CASCADE CONSTRAINTS purge;
###--error 0,600,4043
###drop type t_air;
###--error 0,600,4043
###drop type t_air force;
###--error 0,600,4043
###drop type nvarchar2_100_object;
###--enable_warnings


--disable_warnings
--error 0,942
drop table sales;
--enable_warnings

--disable_warnings
--error 0,942
DROP TABLE DDDTEST;
--enable_warnings


--echo  ##场景测试gb18030-6//字符集涵盖内容gb18030-6-3//gb18030-6-3-1
--echo  ##场景测试gb18030-6//字符集涵盖内容gb18030-6-3//gb18030-6-3-2
--echo  ##场景测试gb18030-6//字符集涵盖内容gb18030-6-3//gb18030-6-3-3
####SELECT 'hello', '"hello"', '""hello""', 'hel''lo', '\'hello','hel\nlo', '"hel\tlo"', '""he\rllo""', 'he\bl''lo', 'he\_l\%lo' from dual;
####SELECT 'This\nIs\nFour\nLines' from dual;
###-- echo --  character-1-1-2  图形符号
select  '?????????????▲???÷↑↓◆◇⊙■□△▽?─│??♂♀?????????▼???≈←→??????????○??◎●???????????⊙??' from dual;
-- echo -- character-1-1-3  图形符号
select  '~@#￥%……&*（）——+：“》？《.+/' from dual;
-- echo --  character-1-2-1-1  简体字
select  '中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国中华人民共和国' from dual;
-- echo --  character-1-2-1-2  繁体字
select  '哎 哀 唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾 爱 砹 隘 嗌 嫒 碍 暧 瑷 僾 薆 安 桉 氨 庵 谙 腤 鹌 鞍 鮟 盫 啽 垵 俺 唵 埯 铵 揞 犴 岸 按 案 胺 暗 黯 肮 昂 枊 盎 醠 凹 坳 垇 爊 敖 嗷 嶅 廒 獒 遨 熬 璈 翱 聱 螯 謷 鳌 鏖 芺 袄 媪 岙 傲 奡 奥' from dual;
--echo  ##场景测试gb18030-6//其他特别关注点gb18030-6-2//gb18030-6-2-1
-- echo --  character-1-2-1-3   中文：双引号+空格/制表符
select  '“”
' from dual;
-- echo --  character-1-2-1-4   简体字和繁体字组合
select  '中华人民共和国中华唉 埃 挨 锿 捱 皑 癌 嗳 矮 蔼 霭 艾' from dual;
-- echo -- character-1-2-2-1   英文大小写
select  'AAAAAAAAAAAADDSGDFHGFJVGHKGHKGHLGLGLHJOLYSGSHBXsdfdsfdsgdsgdfhdfjgfmbvkyuolyhuj;lhjl.jbhn,.dfgdsgsgs' from dual;
-- echo -- character-1-2-2-2   英文：双引号+空格/回车/制表符
select  '""
	' from dual;
-- echo --  character-1-2-2-1   日韩文字
select  '関数内部には問い合わせ文を書くことはできないが,selectにより代入可能である。' from dual;
select  '?? ???? ?? ??? ? ? ??? ???? ?? ?? ?? ? ??.' from dual;
-- echo --  character-1-3-1  苏州码子
select  '苏州码子（〡、〢、〣、〤、〥、〦、〧、〨、〩、十）' from dual;
-- echo --  character-1-3-2-1   正数、负数                          
select  3245356465475674 from dual;
select  -145646547657878 from dual;
-- echo --  character-1-3-2-2   整数，小数          
select  100000000000000000000 from dual;
select  1.32356464212121221646 from dual;
-- echo --   character-1-3-3  罗马数字
select   'Ⅰ Ⅱ Ⅲ Ⅳ Ⅴ Ⅵ Ⅶ Ⅷ Ⅸ Ⅹ Ⅺ Ⅻ' from dual;                         
select 255 from dual;
-- echo --  character-1-4  其他
###--echo  ##场景测试gb18030-6//其他特别关注点gb18030-6-2//gb18030-6-2-4;
select  'defedtgtghgfjfhjf股份附 Ⅴ ? ? ??? ???? ?? ?? ?? ? ??わせ文を Ⅵ Ⅶ Ⅷ 加费苏州码子（放到民共和国中华唉 埃 挨 锿 捱〡、〢、〣、〤、〥、〦、〧、〨、〩、十）' from dual;

--error 900
SELECT 100%89 AS 结果 from dual;


--disable_warnings
--error 0,942
DROP TABLE user002;
--enable_warnings

create table user002(age int, name varchar2(200));

insert into user002 values(21, 'Jack');  
insert into user002 values(23, 'Rose'); 
insert into user002 values(35, '；mn人好久好久生酒馆'); 
insert into user002 values(56, 'VG胡椒粉科技'); 
 
select * from user002 s where age > 30;   
select age as 学生年龄, name as 学生名称 from user002;  

DROP TABLE user002;


--disable_warnings
--error 0,942
DROP TABLE таблица;
--enable_warnings

create table таблица (столбец int, Второй int);
insert into таблица values (1, 1);
insert into таблица values(1, 1);
select столбец+1 嘿嘿, Второй+1 哈哈 from таблица;

--disable_warnings
--error 0,942
DROP TABLE таблица;
--enable_warnings

####补充验证，dba开头的表全量验证

--disable_result_log
select * from nls_database_parameters where PARAMETER='NLS_CHARACTERSET';

select * from dba_objects where rownum <= 10;
select * from dba_tablespaces where rownum <= 10;
--error 0,942
select * from dba_ts_quotas where rownum <= 10;
--error 0,942
select * from dba_free_space where rownum <= 10;
select * from dba_segments where rownum <= 10;
--error 0,942
select * from dba_extents where rownum <= 10;
select * from dba_tables where rownum <= 10;
select * from dba_tab_columns where rownum <= 10;
select * from dba_views where rownum <= 10;
select * from dba_synonyms where rownum <= 10;
select * from dba_sequences where rownum <= 10;
select * from dba_constraints where rownum <= 10;
--error 0,942
select * from dba_indexs where rownum <= 10;
select * from dba_ind_columns where rownum <= 10;
select * from dba_source where rownum <= 10;
--error 0,942
select * from dba_data_files where rownum <= 10;
--error 0,942
select * from dab_tab_grants where rownum <= 10;
--error 0,942
select * from dab_tab_privs where rownum <= 10;
select * from dba_users where rownum <= 10;
select * from DBA_COL_PRIVS where rownum <= 10;
select * from DBA_TAB_PRIVS where rownum <= 10;
select * from DBA_ROLE_PRIVS where rownum <= 10;
select * from DBA_SYS_PRIVS where rownum <= 10;
select * from all_tables where rownum <= 10;
select * from user_tables where rownum <= 10;

--enable_result_log

###补充测试表：DBA_COL_PRIVS/DBA_TAB_PRIVS/DBA_ROLE_PRIVS/DBA_SYS_PRIVS
###补充测试表：dba_xxx,all_tables,user_tables

--error 0,942
drop table t1;
create table t1 as select * from dba_objects where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_tablespaces where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_segments where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_tables where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_tab_columns where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_views where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_synonyms where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_sequences where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_constraints where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_ind_columns where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from dba_source where rownum <= 1;
--error 0,942
drop table t1;
--error 0,1400
create table t1 as select * from dba_users where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from DBA_COL_PRIVS where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from DBA_TAB_PRIVS where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from DBA_ROLE_PRIVS where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from DBA_SYS_PRIVS where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from all_tables where rownum <= 1;
--error 0,942
drop table t1;
create table t1 as select * from user_tables where rownum <= 1;


###select t.username,t.userid,t.password from dba_users t where rownum <= 1;
###select t.username,t.user_id,t.password from dba_users t where rownum <= 1;


--disable_warnings
--error 0,942
drop table t1;
--error 0,600
drop user testdba cascade;
--error 0,2380
drop profile new1_profile;
--enable_warnings


--echo  ########gbk||instr 函数测试##########

--disable_warnings
--error 0,942
drop table gbk_dml_examp;
--error 0,942
drop table gbk_dml_2;
--enable_warnings

create table gbk_dml_examp(c1 varchar(4000),c2 int);
create table gbk_dml_2(c0 varchar(4000),c1 varchar(4000),c2 char(255),c3 varchar(4000));
insert into gbk_dml_examp values('先帝创业未半而中道崩殂，今天下三分，益州疲弊，此诚危急存亡之秋也。然侍卫之臣不懈于内，忠志之士忘身于外者，盖追先帝之殊遇，欲报之于陛下也。诚宜开张圣听，以光先帝遗德，恢弘志士之气，不宜妄自菲薄，引喻失义，以塞忠谏之路也。
宫中府中，俱为一体，陟罚臧否，不宜异同。若有作奸犯科及为忠善者，宜付有司论其刑赏，以昭陛下平明之理，不宜偏私，使内外异法也。
侍中、侍郎郭攸之、费祎、董允等，此皆良实，志虑忠纯，是以先帝简拔以遗陛下。愚以为宫中之事，事无大小，悉以咨之，然后施行，必能裨补阙漏，有所广益。
将军向宠，性行淑均，晓畅军事，试用于昔日，先帝称之曰能，是以众议举宠为督。愚以为营中之事，悉以咨之，必能使行阵和睦，优劣得所。
亲贤臣，远小人，此先汉所以兴隆也；亲小人，远贤臣，此后汉所以倾颓也。先帝在时，每与臣论此事，未尝不叹息痛恨于桓、灵也。侍中、尚书、长史、参军，此悉贞良死节之臣，愿陛下亲之信之，则汉室之隆，可计日而待也。
臣本布衣，躬耕于南阳，苟全性命于乱世，不求闻达于诸侯。先帝不以臣卑鄙，猥自枉屈，三顾臣于草庐之中，咨臣以当世之事，由是感激，遂许先帝以驱驰。后值倾覆，受任于败军之际，奉命于危难之间，尔来二十有一年矣。
先帝知臣谨慎，故临崩寄臣以大事也。受命以来，夙夜忧叹，恐托付不效，以伤先帝之明，故五月渡泸，深入不毛。今南方已定，兵甲已足，当奖率三军，北定中原，庶竭驽钝，攘除奸凶，兴复汉室，还于旧都。此臣所以报先帝而忠陛下之职分也。至于斟酌损益，进尽忠言，则攸之、祎、允之任也。
愿陛下托臣以讨贼兴复之效，不效，则治臣之罪，以告先帝之灵。若无兴德之言，则责攸之、祎、允等之慢，以彰其咎；陛下亦宜自谋，以咨诹善道，察纳雅言，深追先帝遗诏，臣不胜受恩感激。
今当远离，临表涕零，不知所言。;',1);
insert into gbk_dml_2 select c1,c1,substr(c1,1,50),c1 from gbk_dml_examp;

select c0,c1,c2 from gbk_dml_2;
select count(1) from gbk_dml_2;
select length(c0),length(c1),length(c2),length(c3) from gbk_dml_2;
select substr(c0,instr(c0,'自')-1,4),substr(c1,instr(c1,'自')-1,4),substr(c2,instr(c2,'自')-1,4) from gbk_dml_2;
select cast(substr(c0,instr(c0,'自')-1,4) as char(5)) from gbk_dml_2;

--disable_warnings
--error 0,942
drop table gbk_dml_examp;
--error 0,942
drop table gbk_dml_2;
--enable_warnings

--echo  ########gbk||创建函数返回值带中文时##########
--disable_warnings
--error 0,942
drop table  fun_gbk_tb1;
--error 0,600
drop function  fun_gbk;
--enable_warnings

create table fun_gbk_tb1(c1 varchar(400));
insert into fun_gbk_tb1 values('中国之队今日以“TEAM同心齐志”为主题发布赛前海报：“Together，Everyone，Achieves，More，TEAM！我们一起，才能称之为“团队”，同心齐志，才能离目标更近。一同登场，一同拼搏，一同前进！”
　　此前，国足在世预赛中先后大比分击败了马尔代夫与关岛，据悉目前球队已经抵达了菲律宾。(完)');

###delimiter /;
###create or replace function fun_gbk(para1 varchar2,para2 varchar2)
###return varchar2
###is
###dealname varchar2(3000);
###begin
###if length(para1)>length(para2) then  
###dealname := concat(para1,para2);
###elsif length(para1)<length(para2) then  
###dealname := concat(para1,para2);
###else
###dealname :=  para1||'等于'||para2;
###end if;  
###return dealname;
###end;
###/
###delimiter ;/
###
###select fun_gbk(cast('哈哈' as char(2)),cast('哈哈' as char(2))) from dual;
###select fun_gbk(cast(rpad('哈哈',20,'加') as char(20)),cast('哈哈' as char(2))) from dual;
###select cast(rpad('哈哈',20,'加') as char(40)),length(cast(rpad('哈哈',20,'加') as char(40))) from dual;
###select fun_gbk(c1,c1),length(fun_gbk(c1,c1)) from fun_gbk_tb1;
###select fun_gbk(substr(c1,10,30),substr(c1,10,30)),length(fun_gbk(c1,c1)) from fun_gbk_tb1;

--disable_warnings
--error 0,942
drop table  fun_gbk_tb1;
--error 0,600
drop function  fun_gbk;
--enable_warnings

--echo  ########gbk||create role  and user##########
--error 0,600
drop user "哈~"@'%' cascade;
create user "哈~"@'%' identified by 123;

--error 0,600
drop user "哈~" cascade;
create user "哈~" identified by 123;

--error 0,600
drop user "哈~" cascade;
create role "哈1~";

--disable_warnings
--error 0,600
drop user "哈~" cascade;
--error 0,600
drop user "哈~" cascade;
--enable_warnings

--echo  ########嵌套子查询更新时##########
--disable_warnings
--error 0,942
drop table runoob_tbl;
--enable_warnings

CREATE TABLE runoob_tbl(
runoob_id INT PRIMARY KEY,
runoob_title VARCHAR(100),
runoob_author VARCHAR(40),
submission_date DATE
);

INSERT INTO runoob_tbl
(runoob_id, runoob_title, runoob_author, submission_date)
VALUES
(1,'学习 PHP', '菜鸟教程',to_date('2007-12-28 11:07:24','yyyy-mm-dd hh24:mi:ss'));

INSERT INTO runoob_tbl
(runoob_id, runoob_title, runoob_author, submission_date)
VALUES
(2,'学习 MySQL', '菜鸟教程', to_date('2007-12-28 12:07:24','yyyy-mm-dd hh24:mi:ss'));

INSERT INTO runoob_tbl
(runoob_id, runoob_title, runoob_author, submission_date)
VALUES
(3,'JAVA 教程', 'RUNOOB.COM', to_date('2007-12-28 13:07:24','yyyy-mm-dd hh24:mi:ss'));

UPDATE runoob_tbl SET runoob_title='学习 C++' WHERE runoob_id=3;
UPDATE runoob_tbl SET runoob_title='学习 C++', runoob_author = '谁啊' WHERE runoob_id=3;
UPDATE runoob_tbl SET runoob_author = '到底是谁' ;
update runoob_tbl set runoob_title=(select runoob_author from runoob_tbl where runoob_id=3) where exists(select 1 from runoob_tbl where runoob_id=3);

delete from runoob_tbl where exists(select 1 from runoob_tbl where runoob_id=3);

--disable_warnings
--error 0,942
drop table runoob_tbl;
--enable_warnings

--echo  ########gbk||group by rollup##########

--disable_warnings
--error 0,942
drop table employee_tbl;
--enable_warnings

CREATE TABLE employee_tbl(
id int primary key,
name char(10),
datetime DATE,
singin varchar2(10)
);

INSERT INTO employee_tbl VALUES (1, '小明', to_date('2007-12-28 13:07:24','yyyy-mm-dd hh24:mi:ss'), '1');
INSERT INTO employee_tbl VALUES (2, '小王', to_date('2007-12-28 13:07:24','yyyy-mm-dd hh24:mi:ss'), '3');
INSERT INTO employee_tbl VALUES (3, '小丽', to_date('2007-12-28 13:07:24','yyyy-mm-dd hh24:mi:ss'), '2');
INSERT INTO employee_tbl VALUES (4, '小王', to_date('2007-12-28 13:07:24','yyyy-mm-dd hh24:mi:ss'), '4');
INSERT INTO employee_tbl VALUES (5, '小明', to_date('2007-12-28 13:07:24','yyyy-mm-dd hh24:mi:ss'), '4');
INSERT INTO employee_tbl VALUES (6, '小明', to_date('2007-12-28 13:07:24','yyyy-mm-dd hh24:mi:ss'), '2');

SELECT name, COUNT(*) FROM employee_tbl GROUP BY name order by name;
SELECT name, SUM(singin) as singin_count FROM employee_tbl GROUP BY rollup(name, datetime) order by name;
SELECT coalesce(name, '总数'), SUM(singin) as singin_count FROM employee_tbl GROUP BY rollup(name, datetime) order by name;

--disable_warnings
--error 0,942
drop table employee_tbl;
--enable_warnings


--echo  ########gbk||blob、clob##########

--disable_warnings
--error 0,942
drop table gbk_dml_002;
--enable_warnings

create table gbk_dml_002(c0 blob,c1 clob);
--error 1465
insert into gbk_dml_002 values(' 字
龠 ',' 字
龠 ');
--error 1465
insert into gbk_dml_002 values('!@#$%','!@#$%');

insert into gbk_dml_002 values('00101000','abcdefghijklmnopqrstuvwxyz');
insert into gbk_dml_002 values('123D','abcdefghijklmnopqrstuvwxyz');
select * from gbk_dml_002;

--disable_warnings
--error 0,942
drop table gbk_dml_002;
--enable_warnings

--echo  ########gbk||to_char函数不一致##########
--disable_result_log
--error 1858,1841
select to_char(timestamp'now','HH12:MI:SS') from dual;
select to_char(sysdate,'Day, HH12:MI:SS') from dual;
select to_char(sysdate,'Day, HH12:MI:SS') from dual;
###OB报错，oracle正常
###select to_char(sysdate,'FMDay, HH12:MI:SS') from dual;
--enable_result_log
select to_char(125, '999') from dual;
###OB报错，oracle正常
###select to_char(125.8, '999D9') from dual;
select to_date('05 Dec 2000', 'DD Mon YYYY') from dual;
###结果不一致
###select to_timestamp('05 Dec 2000', 'DD Mon YYYY') from dual;
###OB报错，oracle正常
###select to_number('12,454.8-', '99G999D9S') from dual;
select to_char(-0.1,'99.99') from dual;
###OB报错，oracle正常
###select to_char(-0.1,'FM9.99') from dual;
select to_char(0.1,'0.9') from dual;
select to_char(12,'9990999.9') from dual;
###OB报错，oracle正常
###select to_char(12,'FM9990999.9') from dual;
select to_char(485,'999') from dual;
select to_char(-485,'999') from dual;
###OB报错，oracle正常
###select to_char(1485,'9,999') from dual;
###OB报错，oracle正常
###select to_char(1485,'9G999') from dual;
select to_char(148.5,'999.999') from dual;
###OB报错，oracle正常
###select to_char(148.5,'999D999') from dual;
###OB报错，oracle正常
###select to_char(3148.5,'9G999D999') from dual;
###OB报错，oracle正常
###select to_char(-485,'999S') from dual;
###select to_char(-485,'999MI') from dual;
###select to_char(485,'999MI') from dual;
###select to_char(-485,'999PR') from dual;
###select to_char(485,'L999') from dual;
###select to_char(485,'RN') from dual;
###select to_char(485,'FMRN') from dual;
###select to_char(5.2,'FMRN') from dual;
###select to_char(12,'99V999') from dual;
###select to_char(12.4,'99V999') from dual;
###select to_char(12.45, '99V9') from dual;
###Select TO_CHAR(123.0233,'FM9999999.0099') FROM DUAL;
Select TO_CHAR(1.0123) FROM DUAL;
Select TO_CHAR(123) FROM DUAL;
Select TO_CHAR(0.123) FROM DUAL;
###OB报错，oracle正常
###select TO_CHAR (17145,'L099G999','NLS_NUMERIC_CHARACTERS=".," NLS_CURRENCY="NUD"') from dual;
Select TO_CHAR(0.123,'0.999') FROM DUAL;
Select TO_CHAR(100.12,'0.999') FROM DUAL;
Select TO_CHAR(1.12,'0.999') FROM DUAL;
##OB与ORACLE显示结果不一致
##SELECT to_char('Ä Ê Í Ó Ø A B C D E ') from dual;

###select to_char(10,'xxx'), to_char(42,'xxx') from dual;
###select to_number('a','xxx'), to_number('2a','xxx') from dual;
###select to_number('11','xxx') from dual;

--echo  ########gbk||convert##########
###OB暂不支持的语法，后续支持再补充测试
###SELECT CONVERT('strutz','we8hp','f7dec') FROM DUAL;
###SELECT CONVERT('name','us7ascii','zhs16cgb231280') FROM DUAL;
###SELECT CONVERT('Ä Ê Í Ó Ø A B C D E ', 'US7ASCII', 'WE8ISO8859P1') FROM DUAL;
###SELECT CONVERT('Ä Ê Í Ó Ø A B C D E ', 'UTF8', 'zhs16cgb231280') FROM DUAL;
###--error 01482
###SELECT CONVERT('Ä Ê Í Ó Ø A B C D E ', 'UTF8', 'GBK') FROM DUAL;

--echo  ########gbk||##########

--disable_warnings
--error 0,942
drop table gbk_datatype_join1;
--error 0,942
drop table gbk_datatype_join2;
--error 0,942
drop table gbk_datatype_join3;
--enable_warnings

create table gbk_datatype_join2(c1 nvarchar2(2000));
insert into gbk_datatype_join2 values('先帝创业未半而中道崩殂，今天下三分，益州疲弊，此诚危急存亡之秋也。然侍卫之臣不懈于内，忠志之士忘身于外者，盖追先帝之殊遇，欲报之于陛下也。诚宜开张圣听，以光先帝遗德，恢弘志士之气，不宜妄自菲薄，引喻失义，以塞忠谏之路也。
宫中府中，俱为一体，陟罚臧否，不宜异同。若有作奸犯科及为忠善者，宜付有司论其刑赏，以昭陛下平明之理，不宜偏私，使内外异法也。
侍中、侍郎郭攸之、费祎、董允等，此皆良实，志虑忠纯，是以先帝简拔以遗陛下。愚以为宫中之事，事无大小，悉以咨之，然后施行，必能裨补阙漏，有所广益。
将军向宠，性行淑均，晓畅军事，试用于昔日，先帝称之曰能，是以众议举宠为督。愚以为营中之事，悉以咨之，必能使行阵和睦，优劣得所。
亲贤臣，远小人，此先汉所以兴隆也；亲小人，远贤臣，此后汉所以倾颓也。先帝在时，每与臣论此事，未尝不叹息痛恨于桓、灵也。侍中、尚书、长史、参军，此悉贞良死节之臣，愿陛下亲之信之，则汉室之隆，可计日而待也。
臣本布衣，躬耕于南阳，苟全性命于乱世，不求闻达于诸侯。先帝不以臣卑鄙，猥自枉屈，三顾臣于草庐之中，咨臣以当世之事，由是感激，遂许先帝以驱驰。后值倾覆，受任于败军之际，奉命于危难之间，尔来二十有一年矣。
先帝知臣谨慎，故临崩寄臣以大事也。受命以来，夙夜忧叹，恐托付不效，以伤先帝之明，故五月渡泸，深入不毛。今南方已定，兵甲已足，当奖率三军，北定中原，庶竭驽钝，攘除奸凶，兴复汉室，还于旧都。此臣所以报先帝而忠陛下之职分也。至于斟酌损益，进尽忠言，则攸之、祎、允之任也。
愿陛下托臣以讨贼兴复之效，不效，则治臣之罪，以告先帝之灵。若无兴德之言，则责攸之、祎、允等之慢，以彰其咎；陛下亦宜自谋，以咨诹善道，察纳雅言，深追先帝遗诏，臣不胜受恩感激。
今当远离，临表涕零，不知所言。;');
create table gbk_datatype_join1(c1 int,c2 nvarchar2(200),c3 timestamp,c4 date,c5 char(200),c6 varchar2(200),c7 number(6,2),c8 varchar2(2000) generated always as (concat(c1,c2)));

--echo 创建关联表
create table gbk_datatype_join3(c1 int,c2 char(200),c3 timestamp,c4 date,c5 char(200),c6 varchar2(200) ,c7 number(6,2),c8 nvarchar2(2000) generated always as (concat(c1,c2)));

delimiter //;
begin 
for i in reverse 0..30 loop 
insert into gbk_datatype_join1(c1,c2,c3,c4,c5,c6,c7) 
select i,substr(c1,i*10,10),to_date('201909','yyyymm'),to_date('201910','yyyymm'),substr(c1,i*10+1,10),substr(c1,i*10+2,10),i*4+i/7 from gbk_datatype_join2; 
end loop;
end;
//
begin 
for i in reverse 0..30 loop 
insert into gbk_datatype_join3(c1,c2,c3,c4,c5,c6,c7) 
select i,substr(c1,i*20,10),to_date('201910','yyyymm'),to_date('201909','yyyymm'),substr(c1,i*20+1,10),substr(c1,i*20+2,10),i*4+i/7 from gbk_datatype_join2; 
end loop;
end;
//
delimiter ;//

insert into gbk_datatype_join3(c1,c2,c3,c4,c5,c6,c7) select c1,c2,c3,c4,c5,c6,c7 from gbk_datatype_join1 where mod(c1,20)=1;

select a.c1 as c1,a.c2 as c2 from gbk_datatype_join1 a join gbk_datatype_join3 b on a.c2=b.c2 and a.c5=b.c5 and a.c6=b.c6 order by c1 desc;
select * from (select a.c1 as c1,a.c2 as c2 from gbk_datatype_join1 a join gbk_datatype_join3 b on a.c2=b.c2 and a.c5=b.c5 and a.c6=b.c6 order by c1 desc) minus
select * from (select a.c1 as c1,a.c2 as c2 from gbk_datatype_join1 a join gbk_datatype_join3 b on a.c2=b.c2 and a.c5=b.c5 and a.c6=b.c6 order by c1);

select * from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) WHERE c2=c5;

select distinct c2,(select count(*) from gbk_datatype_join1) from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c2=(select c2 from gbk_datatype_join1 where c1=(select max(c1) from gbk_datatype_join1));
select distinct c2,(select count(*) from gbk_datatype_join1,gbk_datatype_join3) from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c2=(select c2 from gbk_datatype_join1 where c1=(select min(c1) from gbk_datatype_join1));
select c2 from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c2=(select c2 from gbk_datatype_join1 where c1=(select avg(c1) from gbk_datatype_join1));
select distinct c5 from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c5=(select c5 from gbk_datatype_join1 where c1=(select max(c1) from gbk_datatype_join1));
select distinct c5 from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c6=(select c6 from gbk_datatype_join1 where c1=(select min(c1) from gbk_datatype_join1));
select distinct c5 from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c2=(select c5 from gbk_datatype_join1 where c1=(select avg(c1) from gbk_datatype_join1));
select distinct c6 from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c6=(select c6 from gbk_datatype_join1 where c1=(select max(c1) from gbk_datatype_join1));
select distinct c6 from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c6=(select c6 from gbk_datatype_join1 where c1=(select min(c1) from gbk_datatype_join1));
select distinct c6 from (select a.c1,a.c2,b.c5,b.c6 from gbk_datatype_join1 a,gbk_datatype_join3 b where a.c3=b.c4) where c6=(select c6 from gbk_datatype_join1 where c1=(select avg(c1) from gbk_datatype_join1));

--disable_warnings
--error 0,942
drop table gbk_datatype_join1;
--error 0,942
drop table gbk_datatype_join2;
--error 0,942
drop table gbk_datatype_join3;
--enable_warnings


--echo  ########gbk||##########
--disable_warnings
--error 0,942
drop table user_test;
--error 0,942
drop table user_test_view;
--error 0,942
drop table user_test_view2;
--enable_warnings

create table user_test(uuuid int,data varchar2(10) primary key);
insert into user_test values (NULL,'沈阳');
insert into user_test values (NULL,'日本');
--error 0,600,54016
alter table user_test add user_name nvarchar2(20) generated always as (data);

insert into user_test(uuuid,data) values (NULL,'四川');

create view user_test_view as select * from user_test;
create view user_test_view2 as select t.uuuid 别名1,t.data 别名2 from user_test t;

###select * from user_test where user_name='沈阳';
select * from user_test;
###select * from user_test_view where user_name='沈阳';
select * from user_test_view;
select * from user_test_view2 where 别名2='沈阳';
select * from user_test_view2;

--disable_warnings
--error 0,942
drop table user_test;
--error 0,942
drop table user_test_view;
--error 0,942
drop table user_test_view2;
--enable_warnings


--echo  ########gbk||##########
--disable_warnings
--error 0,942
drop table test1;
--enable_warnings

create table test1(
id int,
name nvarchar2(32) not null,
email nvarchar2(128),
unique(name,email)
);

insert into test1(id,name,email) values(1,'xxx','qq.com');
insert into test1(id,name,email) values(2,'老王','qq.com');

--disable_warnings
--error 0,942
drop table test1;
--enable_warnings

--echo  ########gbk||##########

--disable_warnings
--error 0,942
drop table test1;
--enable_warnings

create table test1
(
field_name nvarchar2(30) primary key
);

##comment on table test1 is '系统功能人员配置';
##comment on column test1.field_name  is '功能类型 ';

###show create table test1;
###show full columns from test1;

###select * from user_col_comments t where t.TABLE_NAME = 'TEST1' and TRIM(t.COMMENTS) = '功能类型' ;
###select * from user_tab_comments t where t.TABLE_NAME = 'TEST1' and TRIM(t.COMMENTS) = '系统功能人员配置' ;

--disable_warnings
--error 0,942
drop table test1;
--enable_warnings

--echo  ########gbk||##########

--disable_warnings
--error 0,942
drop table t01;
--enable_warnings


create table t01(col1 int,col2 nvarchar2(10));
insert into t01 values(4,'男');

select * from t01;
select length(col2) from t01;

select * from t01 where col2 like '%女%';
select * from t01 where col2 like '%男%';
select * from t01 where col2 like '%1%';
select * from t01 where col2 = '男';

--disable_warnings
--error 0,942
drop table t01;
--enable_warnings

--echo  ########gbk||##########
--disable_warnings
--error 0,942
drop table cast_test_table;
--enable_warnings


create table cast_test_table(
col1 int primary key,
name nvarchar2(10),
sex nvarchar2(10) CHECK(sex='男' or sex='女'),
pep_no_1 int,
city varchar(40) default 'hangzhou',
remark varchar(25)
);

insert into cast_test_table values (1,'',NULL,1,'nanjing','ooo');
--error 0,2290
insert into cast_test_table values (3,'','哈哈',1,'nanjing','ooo');
insert into cast_test_table values (2,'','',1,'nanjing','ooo');
insert into cast_test_table values (4,'','女',1,'nanjing','ooo');

select * from cast_test_table;

--disable_warnings
--error 0,942
drop table cast_test_table;
--enable_warnings

--echo  ########gbk||##########

--disable_warnings
--error 0,942
drop table table0_bigint;
--enable_warnings

create table table0_bigint
(
col_char_20_gb18030 nvarchar2(200),
col_decimal_20_0 number(20,4),
col_char_20_gbk nchar(20) unique,
col_bigint int,
col_char_20_utf8mb4 char(20),
pk nvarchar2(200) primary key,
col_timestamp_6 timestamp
);

insert into table0_bigint values('中文测试001',10,'中文测试001',20,'中文测试1','中文测试1',to_timestamp('20060604', 'yyyymmdd'));
insert into table0_bigint values('中文测试002',10,'中文测试003',20,'中文测试002','中文测试002',to_timestamp('20060604', 'yyyymmdd'));
insert into table0_bigint values('中文测试003',10,'中文测试005',20,'中文测试003','中文测试003',to_timestamp('20060604', 'yyyymmdd'));


UPDATE table0_bigint SET col_char_20_gb18030 = col_char_20_gbk;
SELECT * FROM table0_bigint ORDER BY pk;
SELECT * FROM table0_bigint where col_char_20_gbK not LIKE '%A%';

select pk,col_char_20_gbk,TRIM(col_char_20_gbk||'â') from table0_bigint;

select CONCAT( col_char_20_gb18030 , 'â¡.ãÅã âââãâ0âãâ^âãâÏâãâï¹âãââ³âãââ½â') from table0_bigint;

select * from table0_bigint where ''<= col_char_20_gbk;

--disable_warnings
--error 0,942
drop table table0_bigint;
--enable_warnings


--echo  ########gbk||特殊字符验证##########

--disable_warnings
--error 0,942
drop table table1_float;
--enable_warnings

create table table1_float
(
pk varchar2(200) primary key,
col_bigint_signed varchar2(200),
col_char_20_gbk_unique_key varchar2(200) unique
);

INSERT INTO table1_float ( pk , col_bigint_signed, col_char_20_gbk_unique_key ) VALUES ( 'ΑΒΓΔΕΖΗΘΙΚ∧ΜΝΞΟ∏Ρ∑ΤΥΦΧΨΩ' , SUBSTR( CAST( ( nvl( 2521856, 99 ) ) AS CHAR(100) ) , 1 , 20 ) , '¸¸¼¼<<±±××' );
select * from table1_float;

SELECT PK FROM TABLE1_FLOAT WHERE col_char_20_gbk_unique_key IN ('ΑΒΓΔΕΖΗΘΙΚ∧ΜΝΞΟ∏Ρ∑ΤΥΦΧΨΩ' ,'α β γ δ ε ζ η θ ι κ λ μ ν ξ ο π ρ σ τ υ φ χ ψ ω' ,'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ' ,'а б в г д е ё ж з и й к л м н о п р с т у ф х ц ч ш щ ъ ы ь э ю я' ,'ㄅㄉㄓㄚㄞㄢㄦㄆㄊㄍㄐㄔㄗㄧㄛㄟㄣㄇㄋㄎㄑㄕ ㄘㄨㄜㄠㄤㄈㄏㄒㄖ' ,'ā á ǎ à、ō ó ǒ ò、ê ē é ě è、ī í ǐ ì、ū ú ǔ ù、ǖ ǘ ǚ ǜ ü' ,'ぁぃぅぇぉかきくけこんさしすせそたちつってとゐなにぬねのはひ' ,'ァィゥヴェォカヵキクケヶコサシスセソタチツッテトヰンナニヌネノ' ,'ˉˇ¨‘’々～‖∶”’‘〃〔〕《》「」『』.〖〗【【】()〔〕｝' ,'ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ①②③④⑤⑥⑦⑧⑨⑩一二三 四五六七八九' ,'≈≡≠=≤≥<>≮≯∷±+-×÷/∫∮∝∞∧∨∑∏∪∩∈∵∴⊥‖∠⌒⊙≌∽√' ,'°′〃$￡￥‰%℃¤￠' ,'┌┍┎┏┐┑┒┓—┄┈├┝┞┟┠┡┢┣┆┊┬┭┮┯┰┱┲┳┼┽┾┿╀' ,'§№☆★○●◎◇◆□■△▲※→←↑↓〓&@\^_' ,'⊙●○①⊕◎Θ⊙¤㊣▂ ▃ ▄ ▅ ▆ ▇ █ █ ■ ▓ 回 □ 〓≡ ╝╚╔ ╗╬ ═ ╓ ╩ ┠ ┨┯ ┷┏ ┓┗ ┛┳⊥『』┌♀◆◇◣◢◥▲▼△▽⊿' ,'·⊙①⊕◎Θ⊙●○¤㊣㈱@の■□★☆◆◇◣◢ ◤◥▲△▼▽⊿◢' ,'▂ ▃ ▄ ▅ ▆ ▇ █ ▉ ▊▋▌▍▎▏■ ▓  回 □ 〓≡' ,'↑↓→←↘↙♀♂┇┅‖$ @ * &  ※ 卍 卐 ∞Ψ §∮ № ⌒ *' ,'░ ▒ ▣ ▤ ▥ ▦ ▧ ▨ ▩ ▪ ▫ ▬ ◆ ◇ ◈ ◎ ● ◐ ◑ ☉ ☎ ☏ ☜ ☞ ☺ ☻ ☼ ♠ ♡ ♢ ♣ ♤ ♥ ♦ ♧' ,'.。，、：?!ˉˇ¨`~ 々 ～‖∶·… — ～ - 〃' ,'‘’“”〝〞〔〕〈〉《》「」『』〖〗【】()[]{｝︻︼﹄﹃' ,'+-×÷﹢﹣±/= ∥∠ ≌ ∽ ≦ ≧ ≒﹤﹥ ≈ ≡ ≠ = ≤ ≥ < > ≮ ≯' ,'∷ ∶ ∫ ∮ ∝ ∞ ∧ ∨ ∑ ∏ ∪ ∩ ∈ ∵ ∴ ⊥ ∥ ∠ ⌒ ⊙ √∟⊿ ㏒ ㏑ % ‰' ,'㎎ ㎏ ㎜ ㎝ ㎞ ㎡ ㏄ ㏎ ㏑ ㏒ ㏕ ℡ % ‰ ℃ ℉ °′″$ ￡ ￥ ￠ ♂ ♀℅' ,'① ② ③ ④ ⑤ ⑥ ⑦ ⑧ ⑨ ⑩ ㈠ ㈡ ㈢ ㈣ ㈤ ㈥ ㈦ ㈧ ㈨ ㈩ №' ,'⑴ ⑵ ⑶ ⑷ ⑸ ⑹ ⑺ ⑻ ⑼ ⑽ ⑾ ⑿ ⒀ ⒁ ⒂ ⒃ ⒄ ⒅ ⒆ ⒇' ,'⒈ ⒉ ⒊ ⒋ ⒌ ⒍ ⒎ ⒏ ⒐ ⒑ ⒒ ⒓ ⒔ ⒕ ⒖ ⒗ ⒘ ⒙ ⒚ ⒛' ,'Ⅰ Ⅱ Ⅲ Ⅳ Ⅴ Ⅵ Ⅶ Ⅷ Ⅸ Ⅹ Ⅺ Ⅻ ⅰ ⅱ ⅲ ⅳ ⅴ ⅵ ⅶ ⅷ ⅸ ⅹ' ,'´´©©>>µµ®®' ,'&&°°¡¡  »»' ,'¦¦÷÷¿¿¬¬§§' ,'••½½««¶¶¨¨' ,'¸¸¼¼<<±±××' ,'¢¢¾¾¯¯""™™' ,'♡. 8 ...¤¸¸.·´¯`·.¸·.>>--» 洛雨·晴缘«---<<·.¸¸.·´¯`·.¸¸.¤..' ,'♡.ゃōゃ ⊙▂⊙　⊙0⊙　⊙^⊙　⊙ω⊙　⊙﹏⊙　⊙△⊙　⊙▽⊙' ,'♡.21 ☼ ♦ ▀ ▄ █ ▌ ▐ ░ ▒ ▬ ♦ ◊ ◦ ☼ ♠ ♣ ▣ ▤ ▥' ,'♡.ゃōゃ ⊙▂⊙　⊙0⊙　⊙^⊙　⊙ω⊙　⊙﹏⊙　⊙△⊙　⊙▽⊙' ,'♡. ≥▂≤　≥0≤　≥^≤　≥ω≤　≥﹏≤　≥△≤　≥▽');

--disable_warnings
--error 0,942
drop table table1_float;
--enable_warnings

--echo  ########gbk||##########

--disable_warnings
--error 0,942
drop table 简体用户名;
--error 0,942
drop table 繁體;
--error 0,942
drop table こんにちは;
--error 0,942
drop table Здравствулте;
--error 0,942
drop table 中文名测试_长度上限长长长长长_;
--error 0,942
drop table abcdefghijklmnopqrstuvwxyz;
--error 0,942
drop table ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
--enable_warnings

create table 简体用户名(c1 int);
create table 繁體(c1 int); 
create table こんにちは(c1 int); 
create table Здравствулте(c1 int); 
create table 中文名测试_长度上限长长长长长_(c1 int); 
create table abcdefghijklmnopqrstuvwxyz(c1 int); 
CREATE TABLE ABCDEFGHIJKLMNOPQRSTUVWXYZ__30(c1 int);

SELECT table_name FROM user_tables ORDER BY table_name; 

--disable_warnings
--error 0,942
drop table 简体用户名;
--error 0,942
drop table 繁體;
--error 0,942
drop table こんにちは;
--error 0,942
drop table Здравствулте;
--error 0,942
drop table 中文名测试_长度上限长长长长长_;
--error 0,942
drop table abcdefghijklmnopqrstuvwxyz;
--error 0,942
drop table ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
--enable_warnings

--echo  ##场景测试gb18030-6//其他特别关注点gb18030-6-2//gb18030-6-2-7
################# 对象中文名测试(简体、繁体、超大字符集中汉字、双引号+空格/回车/制表符) #################
--echo 〇（xing/ling）  简体字 繁體  𪚥（zhe）  龘
--echo 用户名
--disable_warnings
--error 0,00600
drop user "哈~" cascade;
--error 0,00600
drop user 简体用户名 cascade;
--error 0,00600
drop user 繁體 cascade;
--error 0,00600
drop user こんにちは cascade;
--error 0,00600
drop user Здравствулте cascade;
--error 0,00600
drop user 中文名测试_长度上限长长长长长_ cascade;
--error 0,00600
drop user "a简体字龘繁體 空格   制表符
aa" cascade;
--error 0,00600
drop user abcdefghijklmnopqrstuvwxyz cascade;
--error 0,00600
DROP USER ABCDEFGHIJKLMNOPQRSTUVWXYZ__30 cascade;
--enable_warnings
create user "哈~" identified by 123;
create user 简体用户名 identified by 简体用户密码;
create user 繁體 identified by 繁體;
create user こんにちは identified by 123;
create user Здравствулте identified by 123;
create user 中文名测试_长度上限长长长长长_ identified by 123;
create user "a简体字龘繁體 空格   制表符
aa" identified by 123;
create user abcdefghijklmnopqrstuvwxyz  identified by abcdefghijklmnopqrstuvwxyz;
--error 01920
CREATE USER ABCDEFGHIJKLMNOPQRSTUVWXYZ  IDENTIFIED BY ABCDEFGHIJKLMNOPQRSTUVWXYZ;
CREATE USER ABCDEFGHIJKLMNOPQRSTUVWXYZ__30  IDENTIFIED BY ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
drop user "哈~" cascade;
drop user 简体用户名 cascade;
drop user 繁體 cascade;
drop user こんにちは cascade;
drop user Здравствулте cascade;
drop user 中文名测试_长度上限长长长长长_ cascade;
drop user "a简体字龘繁體 空格   制表符
aa" cascade;
drop user abcdefghijklmnopqrstuvwxyz cascade;
DROP USER ABCDEFGHIJKLMNOPQRSTUVWXYZ__30 cascade;

--echo 表名
--disable_warnings
--error 0,00942
drop table "哈~";
--error 0,00942
drop table 简体用户名;
--error 0,00942
drop table 繁體;
--error 0,00942
drop table こんにちは;
--error 0,00942
drop table Здравствулте;
--error 0,00942
drop table 中文名测试_长度上限长长长长长_;
--error 0,00942
drop table "a简体字龘繁體 空格   制表符
aa";
--error 0,00942
drop table abcdefghijklmnopqrstuvwxyz;
--error 0,00942
DROP table ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
--enable_warnings
create table "哈~"(c1 int);
create table 简体用户名(c1 int);
create table 繁體(c1 int);
create table こんにちは(c1 int);
create table Здравствулте(c1 int);
create table 中文名测试_长度上限长长长长长_(c1 int);
create table "a简体字龘繁體 空格   制表符
aa"(c1 int);
create table abcdefghijklmnopqrstuvwxyz(c1 int);
--error 00600,955
CREATE TABLE ABCDEFGHIJKLMNOPQRSTUVWXYZ(c1 int);
CREATE TABLE ABCDEFGHIJKLMNOPQRSTUVWXYZ__30(c1 int);
desc "哈~";
desc 简体用户名;
desc 繁體;
desc こんにちは;
desc Здравствулте;
desc 中文名测试_长度上限长长长长长_;
desc "a简体字龘繁體 空格   制表符
aa";
DESC abcdefghijklmnopqrstuvwxyz;
DESC ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
desc ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
drop table "哈~";
drop table 简体用户名;
drop table 繁體;
drop table こんにちは;
drop table Здравствулте;
drop table 中文名测试_长度上限长长长长长_;
drop table "a简体字龘繁體 空格   制表符
aa";
drop table abcdefghijklmnopqrstuvwxyz;
--error 00942
DROP TABLE ABCDEFGHIJKLMNOPQRSTUVWXYZ;
DROP TABLE ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;


--echo 视图名
--disable_warnings
--error 0,00942
--error 0,00942
drop table table_gbk_name_view;
--error 0,00942
drop view 简体用户名;
--error 0,00942
drop view 繁體;
--error 0,00942
drop view こんにちは;
--error 0,00942
drop view Здравствулте;
--error 0,00942
drop view 中文名测试_长度上限长长长长长_;
--error 0,00942
drop view "a简体字龘繁體 空格   制表符
aa";
--error 0,00942
drop view abcdefghijklmnopqrstuvwxyz;
--error 0,00942
DROP VIEW ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
--error 0,00942
DROP VIEW "哈~";
--enable_warnings
create table table_gbk_name_view(c1 int);
create or replace view "哈~" as select * from table_gbk_name_view;
create or replace view 简体用户名 as select * from table_gbk_name_view;
create or replace view 繁體 as select * from table_gbk_name_view;
create or replace view こんにちは as select * from table_gbk_name_view;
create or replace view Здравствулте as select * from table_gbk_name_view;
create or replace view 中文名测试_长度上限长长长长长_ as select * from table_gbk_name_view;
create or replace view "a简体字龘繁體 空格   制表符
aa" as select * from table_gbk_name_view;
create or replace view abcdefghijklmnopqrstuvwxyz as select * from table_gbk_name_view;
--error 00955,00600
CREATE VIEW ABCDEFGHIJKLMNOPQRSTUVWXYZ as select * from table_gbk_name_view;
CREATE VIEW ABCDEFGHIJKLMNOPQRSTUVWXYZ__30 as select * from table_gbk_name_view;
select * from 简体用户名;
select * from 繁體;
select * from こんにちは;
select * from Здравствулте;
select * from 中文名测试_长度上限长长长长长_;
select * from "a简体字龘繁體 空格   制表符
aa";
select * from abcdefghijklmnopqrstuvwxyz;
select * from ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
drop view "哈~";
drop view 简体用户名;
drop view 繁體;
drop view こんにちは;
drop view Здравствулте;
drop view 中文名测试_长度上限长长长长长_;
drop view "a简体字龘繁體 空格   制表符
aa";
drop view abcdefghijklmnopqrstuvwxyz;
--error 00942
DROP VIEW ABCDEFGHIJKLMNOPQRSTUVWXYZ;
DROP VIEW ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;


--echo 同义词名
--disable_warnings
--error 0,00942
drop table table_gbk_name_synonym;
--error 0,00600
drop synonym "哈~";
--error 0,00600
drop synonym 简体用户名;
--error 0,00600
drop synonym 繁體;
--error 0,00600
drop synonym こんにちは;
--error 0,00600
drop synonym Здравствулте;
--error 0,00600
drop synonym 中文名测试_长度上限长长长长长_;
--error 0,00600
drop synonym "a简体字龘繁體 空格   制表符
aa";
--error 0,00600
drop synonym abcdefghijklmnopqrstuvwxyz;
--error 0,00600
DROP SYNONYM ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
--enable_warnings
create table table_gbk_name_synonym(c1 int);
create or replace synonym "哈~" for table_gbk_name_synonym;
create or replace synonym 简体用户名 for table_gbk_name_synonym;
create or replace synonym 繁體 for table_gbk_name_synonym;
create or replace synonym こんにちは for table_gbk_name_synonym;
create or replace synonym Здравствулте for table_gbk_name_synonym;
create or replace synonym 中文名测试_长度上限长长长长长_ for table_gbk_name_synonym;
create or replace synonym "a简体字龘繁體 空格   制表符
aa" for table_gbk_name_synonym;
create or replace synonym abcdefghijklmnopqrstuvwxyz for table_gbk_name_synonym;
--error 00955,00600
CREATE SYNONYM ABCDEFGHIJKLMNOPQRSTUVWXYZ for table_gbk_name_synonym;
CREATE SYNONYM ABCDEFGHIJKLMNOPQRSTUVWXYZ__30 for table_gbk_name_synonym;
select * from 简体用户名;
select * from 繁體;
select * from こんにちは;
select * from Здравствулте;
select * from 中文名测试_长度上限长长长长长_;
select * from "a简体字龘繁體 空格   制表符
aa";
select * from abcdefghijklmnopqrstuvwxyz;
select * from ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;
drop synonym "哈~";
drop synonym 简体用户名;
drop synonym 繁體;
drop synonym こんにちは;
drop synonym Здравствулте;
drop synonym 中文名测试_长度上限长长长长长_;
drop synonym "a简体字龘繁體 空格   制表符
aa";
drop synonym abcdefghijklmnopqrstuvwxyz;
--error 01434,00600
DROP SYNONYM ABCDEFGHIJKLMNOPQRSTUVWXYZ;
DROP SYNONYM ABCDEFGHIJKLMNOPQRSTUVWXYZ__30;

--echo 列名/索引名
--disable_warnings
--error 0,00942
drop table col_name_test_table1;
--error 0,00942
drop table col_name_test_table2;
--error 0,00942
drop table col_name_test_table3;
--error 0,00942
drop table col_name_test_table4;
--error 0,00942
drop table col_name_test_table5;
--error 0,00942
drop table col_name_test_table6;
--error 0,00942
drop table col_name_test_table7;
--error 0,00942
drop table col_name_test_table8;
--error 0,00942
drop table col_name_test_table9;
--error 0,00942
drop table col_name_test_table10;
--error 0,01418
drop index 简体中文;
--error 0,01418
drop index 繁體;
--error 0,01418
drop index "空 格";
--error 0,01418
drop index Здравствулте;
--error 0,01418
drop index こんにちは;
--error 0,01418
drop index bonjour;
--error 0,01418
drop index Hola;
--error 0,01418
drop index hoi;
--enable_warnings
create table col_name_test_table1(简体中文 int);
create table col_name_test_table2(繁體 int);
create table col_name_test_table3("空 格" int);
create table col_name_test_table4(Здравствулте int);
create table col_name_test_table5(こんにちは int);
create table col_name_test_table6("안녕하세요" int);
create table col_name_test_table7(bonjour int);
create table col_name_test_table8("Γειάσου" int);
create table col_name_test_table9(Hola int);
create table col_name_test_table10(hoi int);
insert into col_name_test_table1 values(10);
insert into col_name_test_table2 values(10);
insert into col_name_test_table3 values(10);
insert into col_name_test_table4 values(10);
insert into col_name_test_table5 values(10);
insert into col_name_test_table6 values(10);
insert into col_name_test_table7 values(10);
insert into col_name_test_table8 values(10);
insert into col_name_test_table9 values(10);
insert into col_name_test_table10 values(10);
select 简体中文 from col_name_test_table1;
select 繁體  from col_name_test_table2;
select "空 格"  from col_name_test_table3;
select Здравствулте  from col_name_test_table4;
select こんにちは  from col_name_test_table5;
select bonjour  from col_name_test_table7;
select Hola  from col_name_test_table9;
select hoi  from col_name_test_table10;
select "안녕하세요"  from col_name_test_table6;
select "Γειάσου"  from col_name_test_table8;
--echo indexName
create index 简体中文 on col_name_test_table1(简体中文);
create index 繁體 on col_name_test_table2(繁體);
create index "空 格" on col_name_test_table3("空 格");
create index Здравствулте on col_name_test_table4(Здравствулте);
create index こんにちは on col_name_test_table5(こんにちは);
create index bonjour on col_name_test_table7(bonjour);
create index Hola on col_name_test_table9(Hola);
create index hoi on col_name_test_table10(hoi);
drop index 简体中文;
drop index 繁體;
drop index "空 格";
drop index Здравствулте;
drop index こんにちは;
drop index bonjour;
drop index Hola;
drop index hoi;
--echo 约束名
--disable_warnings
--error 0,00942
drop table col_name_test_table11;
--error 0,00942
drop table col_name_test_table21;
--enable_warnings
--echo checkName->not null  当前ob为一种属性，非约束，无法指定约束名
--echo checkName->unique
create table col_name_test_table11(列1 int,列2 int,列3 int,constraint 唯一约束简体中文 unique(列3));
###BUG
###alter table col_name_test_table11 add constraint "繁體 中文加    空格和制表符" unique(列2);
insert into col_name_test_table11 values('',4,5);
###--error 00001
###insert into col_name_test_table11 values('','',5);
insert into col_name_test_table11 values('',4,'');
###alter table col_name_test_table11 add constraint "繁體\t中文加\n空格和制表符" unique(列1);
insert into col_name_test_table11 values(1,'','');
####--error 02261,01408
####alter table col_name_test_table11 add constraint "繁體\\t中文加\\n空格和制表符" unique(列1);
create table col_name_test_table21(列1 int,列2 int,列3 int,constraint Здравствулте unique(列3));

###BUG
###alter table col_name_test_table21 add constraint こんにちは unique(列2);
insert into col_name_test_table21 values('',4,5);
###--error 00001
###insert into col_name_test_table21 values('','',5);
insert into col_name_test_table21 values('',4,'');
--echo checkName->primary key
--echo checkName->foreign key
--echo checkName->check
--echo 分区名

--echo 生成列名/as 别名
--disable_warnings
--error 0,00942
drop table 生成列1;
--error 0,00942
drop table 生成列2;
--error 0,00942
drop table 生成列3;
--error 0,00942
drop table 生成列4;
--enable_warnings
create table 生成列1(col1 date,col2 int,生成列名测试 date generated always as (col1+col2));
create table 生成列2(col1 date,col2 int,繁體生成列名测试 date generated always as (col1+col2));
create table 生成列3(col1 date,col2 int,"生成 列 空格" date generated always as (col1+col2));
create table 生成列4(col1 date,col2 int,"中文 特殊字符\|/_ 空格" date generated always as (col1+col2));
--echo 单引号命名/双引号命名
--disable_warnings
--error 0,00942
drop table "双引号 命名 测试";
--enable_warnings
create table "双引号 命名 测试"("列 1" int,"繁 體" int);
--echo 引号内带特殊符号( , ' # _ % 等)
--disable_warnings
--error 0,00942
drop table "双引号 %命名_ 测#试";
--enable_warnings
create table "双引号 %命名_ 测#试"("列 ,1" int,"繁 體" int);


##--echo 存储过程名
##--echo 函数名
##--disable_warnings
##--error 0,600,900
##drop procedure 存储过程中文名;
##--error 0,00942
##drop table procedure_table;
##--enable_warnings
##create table procedure_table(c1 int);
##delimiter /;
####create or replace procedure 存储过程中文名 as v_total procedure_table.c1%type; begin v_total :=10; dbms_output.put_line('insert tcp1：'||v_total||' recodes !'); end;
####/
##delimiter ;/
##
##--echo 函数名
##--disable_warnings
##--error 0,00600
##drop function 输出缓冲区内容函数;
##--enable_warnings
##delimiter /;
##create or replace function 输出缓冲区内容函数 return varchar2 is
##r varchar2(4000); t int; r1 varchar2(4000);
##begin
##dbms_output.get_line(r, t);
##while t != 1 loop
##dbms_output.get_line(r1, t);
##r := r || chr(10) || r1;
##end loop;
##return r;
##end;
##/
##delimiter ;/
##--echo 触发器名
##--disable_warnings
##--error 0,00942
##drop table 中文表名;
##--error 0,04080
##drop trigger 触发器中文名测试;
##--error 0,02289
##drop sequence 序列;
##--enable_warnings
##create sequence 序列 increment by 1 start with 1 nomaxvalue minvalue 0 nocycle nocache;
##create table 中文表名(c1 int);
##delimiter /;
##create or replace trigger 触发器中文名测试
##after insert on 中文表名 for each row
##declare
##变量1 number not null default 10;
##變量2 constant number default 10;
##变變 int;
##begin
##变量1 := 100;
##select 序列.nextval into 变變 from dual;
##end;
##/
##delimiter ;/
##insert into 中文表名(c1) values(5);
##select 序列.nextval from dual;
##--echo 包名
##--disable_warnings
##--error 0,00942
##drop table 中文表名;
##--error 0,00600
##drop package 简体中文包名;
##--enable_warnings
##delimiter /;
##create or replace package 简体中文包名
##as
##procedure spStr(str out clob);
##procedure spString(str out clob);
##end 简体中文包名;
##/
##delimiter ;/


--echo bug_id:22774393
--disable_warnings
--error 0,00600
drop user 𪚥 cascade;
--error 0,00600
drop user 안녕하세요 cascade;
--enable_warnings
create user 𪚥 identified by 𪚥;
create user 안녕하세요 identified by 123;

--disable_warnings
--error 0,01418
drop index "안녕하세요";
--error 0,01418
drop index "Γειάσου";
--error 0,00942
drop table gbk_user_indexes_bug;
--enable_warnings
create table gbk_user_indexes_bug(c1 int,c2 int);
create index "안녕하세요" on gbk_user_indexes_bug(c1);
create index "Γειάσου" on gbk_user_indexes_bug(c2);
##--error 00600
##SELECT index_name FROM user_indexes;

--disable_warnings
--error 0,00942
drop table gbk_insert_part4016_bug;
--enable_warnings
create table gbk_insert_part4016_bug(c0 varchar(2),c1 varchar2(20),c2 char(2),列3 int)partition by hash(c1);
insert into gbk_insert_part4016_bug values('','𪚥','',8);
select * from gbk_insert_part4016_bug;

--disable_warnings
--error 0,00942
drop table gbk_22779840;
--enable_warnings
create table gbk_22779840(c1 clob);
insert into gbk_22779840 values('哈哈');
select to_char(c1) from gbk_22779840;

--echo  ##场景测试gb18030-6//字符集涵盖内容gb18030-6-3//gb18030-6-3-4
--echo  ##场景测试gb18030-6//字符集涵盖内容gb18030-6-3//gb18030-6-3-5
--echo  ##场景测试gb18030-6//字符集涵盖内容gb18030-6-3//gb18030-6-3-6
--echo  ########gbk||utf8 - 特殊表名测试+拉丁文/特殊字符##########
###SET NAMES gbk;
###--character_set gbk
--disable_warnings
--error 0,942
drop table Ｔ１;
--error 0,942
drop table Ｔ２;
--error 0,600,942
drop table T3;
--enable_warnings

CREATE TABLE Ｔ１ (Ｃ１ char(200));
CREATE INDEX Ｔ１Ｃ１ ON Ｔ１(Ｃ１);
CREATE TABLE Ｔ２ (Ｃ１ char(200));
CREATE INDEX Ｔ２Ｃ１ ON Ｔ２(Ｃ１);

INSERT INTO Ｔ２ VALUES ('あいうえお');
INSERT INTO Ｔ２ VALUES ('あいうえ');
INSERT INTO Ｔ２ VALUES ('あい');
INSERT INTO Ｔ２ VALUES ('あ');
INSERT INTO Ｔ２ VALUES ('');
INSERT INTO Ｔ２ VALUES ('龔龖龗龞龡');
select * from Ｔ２;
####拉丁文/生僻字
CREATE TABLE T3 (c1 char(20),c2 nchar(20),c3 varchar2(20),c4 nvarchar2(20),c5 clob);
insert into T3 VALUES('ā','ā','ā','ā','ā');
insert into T3 VALUES('ă','ă','ă','ă','ă');
insert into T3 VALUES('ć','ć','ć','ć','ć');
insert into T3 VALUES('ƀ','ƀ','ƀ','ƀ','ƀ');
insert into T3 VALUES('Ƕ','Ƕ','Ƕ','Ƕ','Ƕ');
insert into T3 VALUES('ā','ă','ć','ƀ','Ƕ');
insert into T3 VALUES('黒','黒','黒','黒','黒');
insert into T3 VALUES('匼','匼','匼','匼','匼');
insert into T3 VALUES('','','','','');

select * from T3;
select length(c1),length(c1),length(c1),length(c1),length(c1) from T3;
select lengthb(c1),lengthb(c1),lengthb(c1),lengthb(c1),lengthb(c1) from T3;
select upper(c1),upper(c1),upper(c1),upper(c1),upper(c1) from T3;
select lower(c1),lower(c1),lower(c1),lower(c1),lower(c1) from T3;
select nvl(c1,c2),nvl(c1,c3),nvl(c1,c4),nvl(c1,c5),nvl(c1,c5) from T3;


--disable_warnings
--error 0,942
drop table Ｔ１;
--error 0,942
drop table Ｔ２;
--enable_warnings

purge recyclebin;
