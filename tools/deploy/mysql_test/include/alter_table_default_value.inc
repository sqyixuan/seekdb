--disable_warnings
drop table if exists test;
--enable_warnings

echo 'add column without not null';
create table test(c1 int primary key, c2 int);
insert into test values(1,2);
eval alter table test add column c3 $type;
select c1, c2, hex(c3) from test;

echo 'add column without not null and default value';
eval alter table test add column c10 $type default $value;
select c1, c2, hex(c10) from test;

echo 'add column with not null';
eval alter table test add column c4 $type not null;
select c1, c2, hex(c4) from test;

echo 'add column with not null and default value';
eval alter table test add column c5 $type not null default $value;
select c1, c2, hex(c5) from test;


