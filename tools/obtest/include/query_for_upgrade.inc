connect (conn_obs,$conn_ip,root@obtest,,db1,$conn_port);
##between upgrade, we should close the recyclebin
## or the old server can't not recognize the operation_type OB_DROP_TABLE_TO_RECYCLEBIN
set @@recyclebin = off;
show variables like '%autocommit%';
#DML
--error 0,1062
insert into t1(pk, c1, c2, c3, c4) values(100,'a100','b100','2015-11-11 11:11:11', '2015-12-12 12:12:12');
update t1 set c1='aa2', c2='bb' where pk=2;
delete from t1 where pk=3;
replace into t1(pk, c1, c2, c3, c4) values(100,'a100','b100','2015-11-11 11:11:11', '2015-12-12 12:12:12');
insert into t1(pk, c1, c2, c3, c4) values(100,'a100','b100','2015-11-11 11:11:11', '2015-12-12 12:12:12') on duplicate key update c1='aa100';
select pk,c1,c2,c4 from t1 where pk=1;
select pk,c1,c2,c4 from t1 where pk in (1,2,3);
select pk,c1,c2,c4 from t1 where pk >1 and pk<10;
select pk,c1,c2,c4 from t1;

set autocommit=0;
delete from t1 where pk=1;
select pk,c1,c2,c4 from t1 where pk=1;
--error 0,1062
insert into t1(pk, c1, c2, c3, c4) values(1,'a1','b','2016-11-11 11:11:11', '2016-12-12 12:12:12');
commit;
set autocommit=1;

select pk,c1,c2,c4 from t1;

#DML for increment
select n1 from test_increment;
insert into test_increment(n1) values(3),(4);
insert into test_increment values(3,33) on duplicate key update n1=333;

update test_increment set n1=11 where pk=1;

delete from test_increment where pk=1;
insert into test_increment values(1,11);

replace into test_increment values(2,22);
replace into test_increment(n1) values(5);

select n1 from test_increment;

#注释掉DDL，升级过程中不做table相关的DDL
#DDL
#create table if not exists test(pk int primary key);
#alter table test add column n1 varchar(100);
#show create table test;
#drop table test;

