connection tentsql_conn;
create database moredata;
use moredata;
create table t1(c1 int primary key, c2 int, c3 int, c4 int);
create sequence s1 cache 1000000 noorder;
insert into t1 select s1.nextval, s1.nextval, s1.nextval, s1.nextval from table(generator(100000));
