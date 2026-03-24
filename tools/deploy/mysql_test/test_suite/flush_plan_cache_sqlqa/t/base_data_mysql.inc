-- disable_warnings
drop table  IF EXISTS  gbk_dml_test;
drop table  IF EXISTS  gbk_varchar2_at;
drop table  IF EXISTS  gbk_22774513;
-- enable_warnings

create table gbk_dml_test(c0 varchar(2) character set gbk,c1 varchar(2) character set gbk,c2 char(2) character set gbk,c3 int)DEFAULT CHARACTER SET = gbk;
insert into gbk_dml_test values('1 ','1 ','1 ',1);
insert into gbk_dml_test values('10','10','10',2);
insert into gbk_dml_test values('','','',10);

create table gbk_varchar2_at(c1 varchar(20) character set gbk)DEFAULT CHARACTER SET = gbk;
insert into gbk_varchar2_at values('adsfadsf
\@aasdf');

create table gbk_22774513(c1 varchar(2) character set gbk) DEFAULT CHARACTER SET = gbk ;
alter table gbk_22774513 modify c1 char(100);

select c3 ,c0,c1,c2,length(c2),length(c0),length(c2) lie2,length(c3) lie3 from gbk_dml_test order by c3;
select * from gbk_varchar2_at;


--disable_warnings
drop table if exists t1, t2;
--enable_warnings

create table t1(a int primary key, b int, c int, d int) partition by hash(a) partitions 4;
create index idx_b on t1(b);
create index idx_c on t1(c) partition by hash(c) partitions 4;
create table t2(a int primary key, b int, c int, d int) partition by hash(a) partitions 4;

insert into t1 values (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5);
insert into t2 values (1,1,1,1),(2,2,2,2),(3,3,3,3),(4,4,4,4),(5,5,5,5);