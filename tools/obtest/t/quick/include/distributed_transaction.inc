#############################################
#owner : @handora.qc
#owner group: trans
#description: quick test
# 测试点：验证分布式事务
#############################################
connection conn_mysql;
create table test_dist_tx (test_dist_tx int primary key);
connection conn_sys;
alter system change tenant mysql;
begin;
--error 1044
insert into test.test_dist_tx values(1);
insert into __all_dummy values(1);
commit;
--error 1044
select * from test.test_dist_tx;
select * from __all_dummy;
alter system change tenant sys;
