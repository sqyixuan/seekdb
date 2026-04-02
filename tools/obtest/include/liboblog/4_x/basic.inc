##############
#owner: bohou.ws
#test points: all kinds of tx behave
##############

connection conn_obcdc_mysql;

set ob_query_timeout=90000000;
set ob_trx_timeout=1200000000;
set autocommit=1;

create database if not exists obcdc_db;
ob1.oblog.next;

use obcdc_db;

# normal table with pk
create table basic_normal(a int primary key, b char, c int);
ob1.oblog.next;
insert into basic_normal values (1, 'a', 1);
ob1.oblog.next;

# heap table
create table basic_heap(a int, b int default 2, c int default 3);
ob1.oblog.next;
insert into basic_heap values (1, 1, 1);
ob1.oblog.next;
insert into basic_heap (a, b) values (1, 1);
ob1.oblog.next;
insert into basic_heap (a, c) values (1, 1);
ob1.oblog.next;
insert into basic_heap (b, c) values (1, 1);
ob1.oblog.next;


# default value
create table basic_default_value(a int primary key, b int, c int default 999);
ob1.oblog.next;
insert into basic_default_value (a) values (1);
ob1.oblog.next;
insert into basic_default_value (a, b) values (2, 2);
ob1.oblog.next;
insert into basic_default_value values (6, 6, 6);
ob1.oblog.next;

# pk not at first column
create table basic_pk_last(a int, b int primary key, c int);
ob1.oblog.next;
insert into basic_pk_last values (1, 1, 1);
ob1.oblog.next;
insert into basic_pk_last (a, b) values (1, 2);
ob1.oblog.next;
insert into basic_pk_last (b, c) values (3, 4);
ob1.oblog.next;
insert into basic_pk_last (c, b) values (5, 6);
ob1.oblog.next;
drop database obcdc_db;
ob1.oblog.next;
