#### owner: fangzhou.yfz
#### owner group: IMF
#### description: foreign key references non-unique index in oracle 

--disable_warnings

--error 0, 942
drop table B4;
--error 0, 942
drop table B3;
--error 0, 942
drop table B2;
--error 0, 942
drop table B1;
--error 0, 942
drop table B;
--error 0, 942
drop table A;
--error 0, 942
drop table B;
--enable_warnings

# Oracle forbids to reference to non-unique keys.

create table A(a int, msg int);
create index Aindex on A(a);
--error 2270
create table B(a int, FOREIGN KEY(a) REFERENCES A(a));
drop table A;

create table A(ax int, ay int, primary key(ax, ay));
--error 2270
create table B1(a int, foreign key(a) references A(ax));
--error 2270
create table B2(a int, foreign key(a) references A(ay));
create table B3(ax int, ay int, foreign key(ax, ay) references A(ax, ay));
create table B4(ay int, ax int, foreign key(ay, ax) references A(ay, ax));
insert into A values (1, 10), (1, 20), (30, 1);
insert into B3 values (1, 10);
insert into B4 values (10, 1);
select * from A;
select * from B3;
select * from B4;
drop table B3;
drop table B4;
drop table A;

# Oracle allows to reference primary keys.

create table A(a int primary key, a2 int, foreign key(a2) references A(a));
drop table A;
--error 2270
create table A(a int primary key, a2 int, a3 int, foreign key(a2) references A(a3));
--error 2270
create table A(a int, a2 int, a3 int, primary key (a, a2), foreign key(a3) references A(a));
--error 2270
create table A(a int, a2 int, a3 int, primary key (a2, a), foreign key(a3) references A(a));

create table A(a int, a2 int, a3 int, a4 int, primary key (a, a2), foreign key(a3, a4) references A(a, a2));
insert into A values (1, 1, NULL, NULL);
insert into A values (1, 2, 1, 1);
select * from A;
drop table A;

# Oracle allows to reference unique keys.

create table A(a int primary key, a2 int, a3 int unique, foreign key(a2) references A(a3));
drop table A;