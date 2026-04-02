set ob_enable_plan_cache=0;
######################## test no other condition #######################
--error 0, 942
drop table test_mj1;
--error 0, 942
drop table test_mj2;
create table test_mj1(c1 int, c2 int, primary key(c1,c2));
create table test_mj2(d1 int, d2 int, primary key(d1, d2));

--echo 1. test one child is empty
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

insert into test_mj1 values(-3, -30);
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

insert into test_mj1 values(-2, -20);
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

insert into test_mj1 values(-1, -10);
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

insert into test_mj1 values(0, 0);
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

insert into test_mj1 select level, level * 10 from dual connect by level < 1000;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

--echo 2. test no match rows
delete from test_mj1;
insert into test_mj1 select 3 * level, 1 from dual connect by level < 65;
insert into test_mj2 select 3 * level + 1, 1 from dual connect by level < 65;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

delete from test_mj2;
insert into test_mj2 select level + 1000, 1 from dual connect by level < 65;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

--echo 3.1 test all rows of both sides match
delete from test_mj1;
delete from test_mj2;
insert into test_mj1 select level * 3, 1 from dual connect by level < 65;
insert into test_mj2 select level * 3, 1 from dual connect by level < 65;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);


--echo 3.2 test all rows of one side match
delete from test_mj2;
insert into test_mj2 select level * 3, 1 from dual connect by level < 150;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

delete from test_mj2;
insert into test_mj2 select level * 3, 1 from dual connect by level < 65;
insert into test_mj2 select level * 3 + 1, 1 from dual connect by level < 65;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

insert into test_mj2 select level + 1000, 1 from dual connect by level < 150;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);


--echo 3.2 test some rows match
delete from test_mj2;
insert into test_mj2 select level * 3, 1 from dual connect by level < 65;
insert into test_mj2 select level * 4, 2 from dual connect by level < 65;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

delete from test_mj2;
insert into test_mj2 select level * 11, 1 from dual connect by level < 65;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

insert into test_mj1 select level + 10000, 3 from dual connect by level < 65;
insert into test_mj2 select level + 20000, 3 from dual connect by level < 65;
insert into test_mj1 select level * 5 + 30000, 4 from dual connect by level < 65;
insert into test_mj2 select level * 13 + 30000, 4 from dual connect by level < 65;
select * from test_mj1 join test_mj2 on c1 = d1;
select * from test_mj2 join test_mj1 on c1 = d1;
select * from test_mj1 left join test_mj2 on c1 = d1;
select * from test_mj2 left join test_mj1 on c1 = d1;
select /*+ leading(test_mj1, test_mj2)*/ * from test_mj1 right join test_mj2 on c1 = d1;
select /*+ leading(test_mj2, test_mj1)*/ * from test_mj2 right join test_mj1 on c1 = d1;
select * from test_mj1 full outer join test_mj2 on c1 = d1;
select * from test_mj2 full outer join test_mj1 on c1 = d1;
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 in (select c1 from test_mj1);
select /*+ use_merge(test_mj1, test_mj2) */* from test_mj1 where c1 not in (select d1 from test_mj2);
select /*+ use_merge(test_mj2, test_mj1) */* from test_mj2 where d1 not in (select c1 from test_mj1);

drop table test_mj1;
drop table test_mj2;
