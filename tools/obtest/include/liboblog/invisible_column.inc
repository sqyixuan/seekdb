######################################################
# 测试 invisible 列定义为 not null 属性的情况
create table t1(c1 int invisible not null, c2 int);
ob1.oblog.next;
insert into t1(c1, c2) values(1, 2);
ob1.oblog.next;

######################################################
# 测试 invisible 列不为 not null （default null）的情况
create table t2(c1 int invisible, c2 int);
ob1.oblog.next;
insert into t2 values(2);
ob1.oblog.next;
insert into t2(c1, c2) values(1, 2);
ob1.oblog.next;

# 测试主键 invisible
create table t3(c1 int primary key, c2 int);
ob1.oblog.next;
insert into t3 values(1, 1);
ob1.oblog.next;

alter table t3 modify c1 invisible;
ob1.oblog.next;
insert into t3(c1, c2) values(2, 2);
ob1.oblog.next;
insert into t3(c1, c2) values(3, 3);
ob1.oblog.next;
alter table t3 modify c1 visible;
ob1.oblog.next;
insert into t3 values(4, 4);
ob1.oblog.next;
insert into t3 values(5, 5);
ob1.oblog.next;

# 测试 add invisible column
create table t4(c1 int invisible, c2 char(10));
ob1.oblog.next;
######################################################
###### 测试写入非invisible列/测试指定invisible写入
# write c2
insert into t4 values(1);
ob1.oblog.next;
# write c2
insert into t4 values(2);
ob1.oblog.next;
# 指定invisible写入
insert into t4(c1, c2) values(3, 3);
ob1.oblog.next;
insert into t4(C1, C2) values(4,4);
ob1.oblog.next;

######################################################
##### 测试修改列invisible属性后
# c1->visible c2->invisible
alter table t4 modify c1 visible, modify c2 invisible;
ob1.oblog.next;
# write c1
insert into t4 values(5);
ob1.oblog.next;
insert into t4(C1, C2) values(6, 6);
ob1.oblog.next;
insert into t4 values(100);
ob1.oblog.next;
insert into t4(C1, C2) values(101, 101);
ob1.oblog.next;

alter table t4 add c3 int invisible default 123;
ob1.oblog.next;
alter table t4 add c4 int invisible unique;
ob1.oblog.next;

# c1可间，c2,c3,c4->invisible
# 测试 insert into invisible column
insert into t4(c1, c2, c3) values(3, 456, 78910);
ob1.oblog.next;

# 唯一索引visible
alter table t4 modify c4 visible;
ob1.oblog.next;
# write c1, c4
insert into t4 values(102, 555);
ob1.oblog.next;
insert into t4(c1, c2, c3, c4) values(4, 457, 78911, 666);
ob1.oblog.next;
