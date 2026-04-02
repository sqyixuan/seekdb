desc t20181202;
--source mysql_test/include/show_create_table_old_version_replica2.inc
show create table t20181202;

select * from t20181202 order by c1;
insert into t20181202 (c1) values (sjhseq.nextval);
insert into t20181202 (c1) values (sjhseq.nextval);
select * from t20181202 order by c1;

