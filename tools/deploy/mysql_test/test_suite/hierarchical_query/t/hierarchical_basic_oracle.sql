set wrap off;
set linesize 300;
set pagesize 200;
set echo on;
drop table emp;
drop table t1;
drop table emp_cycle;

--setup data
create table t1(c1 int);
insert into t1 values(1);
insert into t1 values(3);
insert into t1 values(5);
insert into t1 values(7);
insert into t1 values(9);

create table emp(emp_id int primary key, emp_name varchar(20), mgr_id int);
insert into emp values(1, '1', 0);
insert into emp values(2, '1.2', 1);
insert into emp values(3, '1.2.3', 2);
insert into emp values(4, '1.4', 1);
insert into emp values(5, '1.4.5', 4);
insert into emp values(6, '1.4.6', 4);
insert into emp values(7, '1.4.6.7', 6);
insert into emp values(8, '1.4.8', 4);
insert into emp values(9, '1.9', 1);
insert into emp values(10, '1.9.10', 9);

create table emp_cycle(emp_id int, emp_name varchar(20), mgr_id int);
insert into emp_cycle select * from emp;
insert into emp_cycle values(1, '1.9.10.1', 10);
-- select emp_id, emp_name, mgr_id, level from emp start with emp_id = 1 connect by prior emp_id = mgr_id;

--
--###################### test for start with ################################
--##### without start with #####
--
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
connect by prior emp_id = mgr_id;
--
--##### start with get 1 row #####
--
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 2 connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 4 connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 6 connect by prior emp_id = mgr_id;

--
--##### start with get multi row #####
--
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 or emp_id = 2 connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id > 8 connect by prior emp_id = mgr_id;

--
--##### start with contain correlated/uncorrelated subquery #####
--
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = (select max(c1) from t1 where c1 > emp_id) connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = (select min(c1) from t1 where c1 > emp_id) connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = (select max(c1) from t1) connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = (select min(c1) from t1) connect by prior emp_id = mgr_id;

--
--##### start with pseudocolumn #####
--

--level 在start with取0值

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with level = 0 connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with level < 0 connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with level > 0 connect by prior emp_id = mgr_id;

--CONNECT_BY_ISCYCLE在start with中是非法的

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with CONNECT_BY_ISCYCLE = 0 connect by nocycle prior emp_id = mgr_id;

--CONNECT_BY_ISCYCLE在start with中是非法的

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with CONNECT_BY_ISLEAF = 0 connect by nocycle prior emp_id = mgr_id;

--SYS_CONNECT_BY_PATH在start with中是非法的

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with SYS_CONNECT_BY_PATH(emp_name, '/') = "abc" connect by nocycle prior emp_id = mgr_id;

--
--##### start with test for related operator #####
--

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with prior emp_id = 1 connect by nocycle prior emp_id = mgr_id;

--ERROR : CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with connect_by_root emp_name > 'a' connect by nocycle prior emp_id = mgr_id;



--
--##### start with aggregation expression #####
--
-- start with中不许使用聚集函数

select emp_id, emp_name, mgr_id, level  from emp
start with emp_id = max(8) connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level from emp
start with emp_id = max(emp_id) connect by prior emp_id = mgr_id;

--
--###################### test for connect by ################################

--
--##### without connect by is wrong #####
--
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1;

--
--##### without prior #####
--

-- 结果集出现了循环，反而在结果中没有判断出循环 ???

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "Cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by NOCYCLE (emp_id = 1 or emp_id = 2) and level < 4;

--
--##### with one prior #####
--
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by prior emp_id = mgr_id;

--
--##### with multi prior #####
--

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by prior emp_id = mgr_id and prior emp_id = emp_id - 1;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by (prior emp_id = mgr_id or prior emp_id = emp_id - 1) and level < 5;

--
--##### with const in prior expression #####
--

--结果想不通:存在循环，但是非预期内的循环 ???

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by nocycle prior 1 = mgr_id;

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by nocycle prior emp_id = 1;

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by nocycle prior mgr_id = 1;

--结果想不通:存在循环，但是非预期内的循环 ???
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by nocycle prior 1 = 1 and level < 3;

--
--##### with pseudocolumn #####
--

--level 出现在connect by condition 中时，认为是child的level
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id and level = 1;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id and level = 2;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id and level = 3;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by  prior emp_id = mgr_id and (level = 3 or level = 2);

--level 出现在prior 中时，认为是parent的level，root时则为0
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle prior emp_id = level + 1 ;

--error Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by  emp_id = prior level = mgr_id ;

--CONNECT_BY_ISCYCLE:error Specified pseudocolumn or operator not allowed here.

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by CONNECT_BY_ISCYCLE "cycle"  prior emp_id = mgr_id ;

--CONNECT_BY_ISLEAF:error Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by CONNECT_BY_ISLEAF  prior emp_id = mgr_id ;

--CONNECT_BY_ISLEAF:error Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior CONNECT_BY_ISLEAF = mgr_id ;

--CONNECT_BY_ISLEAF:error Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = CONNECT_BY_ISLEAF ;

--SYS_CONNECT_BY_PATH:SYS_CONNECT_BY_PATH function is not allowed here

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by SYS_CONNECT_BY_PATH(emp_name, '/')  prior emp_id = mgr_id ;

--
--##### with related operator #####
--
--ERROR : CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id  and connect_by_root emp_name > 'a';

--ERROR : CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = connect_by_root emp_name;

--
--##### with aggregation function in prior expression or connect by condition #####
--

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior max(emp_id) = mgr_id ;

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = max(mgr_id) ;

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id and max(emp_id) = 1 ;

--
--##### with correlated/uncorrelated subqury in prior expression or connect by condition #####
--

-- uncorrelated subquery
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle  prior emp_id = (select min(c1) from t1);

select emp_id, emp_name, mgr_id, level,  CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle prior (select min(c1) from t1) = mgr_id;

select emp_id, emp_name, mgr_id, level,  CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle prior emp_id = mgr_id and level  > (select min(c1) from t1);

-- correlated subquery,支持???,结果是否合理

select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle  prior emp_id = (select min(c1) from t1 where c1 > emp_id);

select emp_id, emp_name, mgr_id, level,  CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle prior (select min(c1) from t1 where c1 > emp_id) = mgr_id;

select emp_id, emp_name, mgr_id, level,  CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle prior emp_id = mgr_id and level  > (select min(c1) from t1 where c1 > emp_id);

--
--##### test filter result of connect by expression #####
--

-- filter the child emp_id, all the child of emp_id == 2 will be filtered

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id and emp_id != 2;

-- filter the root emp_id

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id and emp_id != 1;

select emp_id, emp_name, mgr_id, level,  CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by nocycle prior emp_id = mgr_id or mgr_id = 9;

--
--##### test for complex expression #####
--
select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by (1 + prior emp_id) = (mgr_id + 1);


--
--###################### test for cycle ################################
--

--ERROR: nocyle必须在connect by之后
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id nocycle;

--ERROR:CONNECT_BY_ISCYCLE单独出现的情况, NOCYCLE keyword is required with CONNECT_BY_ISCYCLE pseudocolumn
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by prior emp_id = mgr_id;


-- --ERROR: 会出现无限循环的情况, oracle 会一直hang住,因为connect by expression中不含有prior expr
-- select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle"  from emp where level < 3
-- start with emp_id =1 connect by nocycle mgr_id > 1;

--与上面例子的区别是connect by express中含有prior expr, 这样可以通过emp_id相同来终止循环
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle"  from emp where level < 3
start with emp_id =1 connect by nocycle prior emp_id = 1 or  mgr_id > 1;

--connect by 中仅有prior expression时符合预期
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle"  from emp
start with emp_id =1 connect by nocycle prior mgr_id = 0;

--## 测试判断循环的标准，是row相关的，还是prior expression值相关的##
select emp_id, emp_name, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp_cycle
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;

drop table emp_cycle_2;
create table emp_cycle_2(emp_id1 int, emp_id2 int, mgr_id int);
insert into emp_cycle_2 values(1, 5, 0);
insert into emp_cycle_2 values(2, 5, 6);
insert into emp_cycle_2 values(3, 3, 7);

select emp_id1, emp_id2, mgr_id, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_id1, '/') "Path"
from emp_cycle_2
start with emp_id1 + emp_id2 = 6
connect by nocycle prior (emp_id1 + emp_id2) = mgr_id;

--## (3, 4, 1, 5)出现循环，因为row1与row3 prior expression相同(即两个row的emp_id1和emp_id2相同)
drop table emp_cycle_3;
create table emp_cycle_3(emp_id1 int, emp_id2 int, mgr_id1 int, mgr_id2 int);
insert into emp_cycle_3 values(6, 5, -1, -1);
insert into emp_cycle_3 values(3, 4, 1, 5);
insert into emp_cycle_3 values(6, 5, 3, -1);

select emp_id1, emp_id2, mgr_id1, mgr_id2, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_id1, '/') "Path"
from emp_cycle_3
start with emp_id1 = 1 or emp_id1 = 6
connect by nocycle prior emp_id1 = mgr_id1 or prior emp_id2 = mgr_id2;

--## row1和row2并不会出现循环，因为他们的emp_id1不同
drop table emp_cycle_4;
create table emp_cycle_4(emp_id1 int, emp_id2 int, mgr_id1 int, mgr_id2 int);
insert into emp_cycle_4 values(1, 5, -1, -1);
insert into emp_cycle_4 values(3, 4, 1, 5);
insert into emp_cycle_4 values(6, 5, 3, -1);

select emp_id1, emp_id2, mgr_id1, mgr_id2, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_id1, '/') "Path"
from emp_cycle_4
start with emp_id1 = 1 or emp_id1 = 6
connect by nocycle prior emp_id1 = mgr_id1 or prior emp_id2 = mgr_id2;

drop table emp_cycle_5;
create table emp_cycle_5(emp_id int, mgr_id int, emp_name varchar(20));
insert into emp_cycle_5 values(1, 0, 'first_emp');
insert into emp_cycle_5 values(2, 1, 'second_emp');
insert into emp_cycle_5 values(-1, 2, 'third_emp');

select emp_id, mgr_id, emp_name, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path"
from emp_cycle_5
start with abs(emp_id) = 1
connect by nocycle prior abs(emp_id) = mgr_id;

select emp_id, mgr_id, emp_name, level, CONNECT_BY_ISCYCLE "cycle", SYS_CONNECT_BY_PATH(emp_name, '/') "Path"
from emp_cycle_5
start with abs(emp_id) = 1
connect by nocycle abs(prior emp_id) = mgr_id;

--
--###################### test for group by / order by  ################################
--

--### test for group by ###

select max(emp_id) from emp
start with emp_id =1 connect by  prior emp_id = mgr_id group by level;

select max(emp_id) from emp
start with emp_id =1 connect by  prior emp_id = mgr_id group by emp_id having max(level) > 1;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by CONNECT_BY_ISCYCLE;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by emp_id having max(CONNECT_BY_ISCYCLE) = 0;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by CONNECT_BY_ISLEAF;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by emp_id having max(CONNECT_BY_ISLEAF) = 0;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by connect_by_root emp_name;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by emp_id having max(connect_by_root emp_name) > '0';

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by prior emp_id;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by emp_id;

select max(emp_id) from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id group by emp_id having max(prior emp_id) != 4;

--ERROR : SYS_CONNECT_BY_PATH function is not allowed here
select max(SYS_CONNECT_BY_PATH(emp_name, '/')) from emp
start with emp_id =1 connect by nocycle prior emp_id = mgr_id group by SYS_CONNECT_BY_PATH(emp_name, '/');

--ERROR : SYS_CONNECT_BY_PATH function is not allowed here
select max(SYS_CONNECT_BY_PATH(emp_name, '/')) from emp
start with emp_id =1 connect by nocycle prior emp_id = mgr_id group by emp_id having max(SYS_CONNECT_BY_PATH(emp_name, '/')) > 0;


--## test for order by ##

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order by emp_id desc;

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id order by CONNECT_BY_ISCYCLE desc;

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order by CONNECT_BY_ISLEAF desc;

select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order by SYS_CONNECT_BY_PATH(emp_name, '/') desc;

select emp_id, emp_name, mgr_id, level,  connect_by_root emp_name, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order by connect_by_root emp_name desc;

select emp_id, emp_name, mgr_id, level,  connect_by_root emp_name, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order by prior emp_id desc;

--## test for order siblings by ##
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by emp_id desc;

--ERROR : ORDER SIBLINGS BY clause not allowed here
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
order siblings by emp_id desc
start with emp_id =1 connect by  prior emp_id = mgr_id ;

--ERROR:Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by level desc;

--ERROR:Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by CONNECT_BY_ISLEAF desc;

--ERROR:Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  nocycle prior emp_id = mgr_id order siblings by CONNECT_BY_ISCYCLE desc;
start with emp_id = 1 connect by prior emp_id = mgr_id  and connect_by_root emp_name > 'a';
--ERROR:CONNECT BY ROOT operator is not supported in the START WITH or in the CONNECT BY condition
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by connect_by_root emp_name desc;

--ERROR:Specified pseudocolumn or operator not allowed here.
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by prior emp_id desc;

--ERROR:SYS_CONNECT_BY_PATH function is not allowed here
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by SYS_CONNECT_BY_PATH(emp_name, '/') desc;

--uncorrelated subquery in order by
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by (select c1 from t1 where c1 = 1) desc;

--correlated subquery in order by
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id =1 connect by  prior emp_id = mgr_id order siblings by (select min(c1) from t1 where c1 > emp_id) desc;

--
--###################### test for join / view  ################################
--

-- hierarchical query from view
select emp_id, emp_name, mgr_id, level,  SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from (select * from emp)
start with emp_id =1 connect by  prior emp_id = mgr_id;

-- hierarchical query from join relation
-- ??? 仅一行非法的输出

select a.emp_id, a.emp_name, a.mgr_id, level,  SYS_CONNECT_BY_PATH(a.emp_name, '/') "Path"
from emp a join emp b on a.emp_id = b.emp_id
start with a.emp_id =1 connect by  prior a.emp_id = a.mgr_id;

drop table t2;
drop table t3;
create table t2(id int, f_id int);
insert into t2 values(1,1);
insert into t2 values(2,2);
insert into t2 values(3,3);
insert into t2 values(4,4);
create table t3(id int, p_id int);
insert into t3 values(1,0);
insert into t3 values(2,1);
insert into t3 values(3,1);
insert into t3 values(4,2);

select tt2.id, tt3.id, tt3.p_id, level from  t2 tt2 join t3 tt3 on tt2.f_id = tt3.id
start with tt2.id = 1 connect by prior tt2.id = tt3.p_id;

-- level在join中为0值
select tt2.id, tt3.id, tt3.p_id, level from t2 tt2 join t3 tt3 on tt2.f_id = tt3.id and level = 0
start with tt2.id = 1 connect by prior tt2.id = tt3.p_id;

select tt2.id, tt3.id, tt3.p_id, level from t2 tt2 join t3 tt3 on tt2.f_id = tt3.id and level = 1
start with tt2.id = 1 connect by prior tt2.id = tt3.p_id;

--
--###################### test for connect_by_root operator ################################
--
select emp_id, emp_name, mgr_id, level, connect_by_root emp_name "manager", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with emp_id = 1 connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, connect_by_root emp_name "manager", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with (emp_id = 1 or emp_id = 2) connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, connect_by_root emp_name "manager", SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
start with (emp_id = 1 or emp_id = 2 or emp_id = 3 or emp_id = 6) connect by prior emp_id = mgr_id;

select max(emp_id) from emp
start with (emp_id = 1 or emp_id = 2) connect by prior emp_id = mgr_id group by connect_by_root emp_name;

--
--###################### test for prior operator ################################
--
select emp_id, level, prior emp_id from emp
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;

--ERROR : CONNECT BY clause required in this query block
select emp_id, prior emp_id from emp;

--ERROR : Specified pseudocolumn or operator not allowed here.
select emp_id, level, prior emp_id, prior (prior emp_id) from emp
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;

--test for root row prior column
select emp_id, prior emp_id from emp where prior emp_id IS NULL start with emp_id = 1 connect by prior emp_id = mgr_id;

--##### test for prior expression param #####
select emp_id, mgr_id, level , prior (SYS_CONNECT_BY_PATH(emp_name, '/')) from emp start with emp_id = 1 connect by prior emp_id = mgr_id;

--ERROR OBE-00976: Specified pseudocolumn or operator not allowed here.
select emp_id, mgr_id, level , prior (ROWNUM) from emp start with emp_id = 1 connect by prior emp_id = mgr_id;

select emp_id, mgr_id, level , prior (CONNECT_BY_ROOT emp_id) from emp start with emp_id = 1 connect by prior emp_id = mgr_id;

--
--###################### test for where condition  ################################
--

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where emp_id != 4
start with emp_id = 1
connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where level != 2
start with emp_id = 1
connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where CONNECT_BY_ISLEAF = 0
start with emp_id = 1
connect by prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where CONNECT_BY_ISCYCLE = 0
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where SYS_CONNECT_BY_PATH(emp_name, '/')  > '0'
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where connect_by_root emp_name > '0'
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where prior emp_id != 4
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;

select emp_id, emp_name, mgr_id, level, SYS_CONNECT_BY_PATH(emp_name, '/') "Path" from emp
where prior emp_id != 6
start with emp_id = 1
connect by nocycle prior emp_id = mgr_id;




exit;
