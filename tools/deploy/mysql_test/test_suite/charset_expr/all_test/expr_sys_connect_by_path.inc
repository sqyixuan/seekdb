--error 0,942
drop table emp;
create table emp (empno nchar(5), ename nchar(8), mgr nchar(8));
insert into emp (EMPNO, ENAME, MGR)
values ('1', 'jim', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('2', 'tom', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('3', 'tim', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('4', 'lily', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('5', 'mary', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('6', 'tid', '7');
insert into emp (EMPNO, ENAME, MGR)
values ('7', 'King', '10');
insert into emp (EMPNO, ENAME, MGR)
values ('8', 'kenvin', '10');
insert into emp (EMPNO, ENAME, MGR)
values ('9', 'shema', '8');
insert into emp (EMPNO, ENAME, MGR)
values ('10', 'john', '0');
insert into emp (EMPNO, ENAME, MGR)
values ('0', 'root', '');
select sys_connect_by_path(ename,'>') tree from emp start with ename='King' connect by prior empno = mgr;
select sys_connect_by_path(ename,'/') tree from emp start with ename='King' connect by prior empno = mgr;
drop table emp;