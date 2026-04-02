--error 0,942
drop table t0;
create table t0(c0 int,c1 int)partition by hash(c0) partitions 4;

insert into t0 values(0,0);
insert into t0 values(1,1);
insert into t0 values(2,2);
insert into t0 values(3,3);

create or replace package pkg1 as
type tb is table of t0%rowtype index by varchar2(10);
type ttb is table of tb;
t ttb:=ttb();
procedure p1(a t0%rowtype, b t0%rowtype);
procedure p2;
procedure p3;
end;
/

create or replace package body pkg1 as
procedure p1(a t0%rowtype, b t0%rowtype) is
v tb;
begin
v('old'):=a;
v('new'):=b;
t.extend();
t(t.count):=v;
end;
procedure p2 is
begin
pkg1.t.delete(pkg1.t.count);
end;
procedure p3 is
begin
for i in 1..100 loop
  if mod(i,2)=1 then
    pkg1.t(i).delete('old');
  end if;
end loop;
end;
end;
/

drop table t0;
drop package pkg1;