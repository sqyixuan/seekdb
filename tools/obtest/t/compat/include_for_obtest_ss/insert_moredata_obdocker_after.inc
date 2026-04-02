connection tentsql_conn;
SET ob_query_timeout = 20000000000;
use moredata;
create table t2(c1 int primary key, c2 int, c3 int, c4 int);
create sequence s2 cache 1000000 noorder;
insert into t2 select s2.nextval, s2.nextval, s2.nextval, s2.nextval from table(generator(100000));
insert into t1 select s1.nextval, s1.nextval, s1.nextval, s1.nextval from table(generator(100000));
