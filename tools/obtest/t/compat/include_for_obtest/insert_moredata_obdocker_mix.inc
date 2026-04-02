connection tentsql_conn;
use moredata;
insert into t1 select s1.nextval, s1.nextval, s1.nextval, s1.nextval from table(generator(100000));
