--disable_warnings
--error 0, 942
drop table sourceTable;
--error 0, 942
drop table targetTable;
--error 0, 942
drop view targetView;
--enable_warnings
--result_format 4

create table sourceTable(id int, sales int);
create table targetTable(id int, sales int);
create view targetView as select * from targetTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);
insert into targetTable values(3,3);

insert into sourceTable values(1,2);
insert into sourceTable values(2,4);
insert into sourceTable values(3,6);
insert into sourceTable values(4,8);

###################### test for update clause ###################

########## 1 target row vs 1 source row ########
explain merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);
select * from sourceTable;

######## 1(t) vs n(s) match #######
truncate table sourceTable;
truncate table targetTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);

insert into sourceTable values(1,2);
insert into sourceTable values(-1,-2);
insert into sourceTable values(2,4);
insert into sourceTable values(-2,-4);

# OBE-30926: unable to get a stable set of rows in the source tables
--error 30926
merge into targetTable t1 using sourceTable t2 on (t1.id = abs(t2.id)) when matched then update set t1.sales = t2.sales when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);
select * from targetTable;

######## n(t) vs 1(s) match #######
truncate table sourceTable;
truncate table targetTable;

insert into sourceTable values(1,1);
insert into sourceTable values(2,2);

insert into targetTable values(1,2);
insert into targetTable values(-1,-2);
insert into targetTable values(2,4);
insert into targetTable values(-2,-4);
merge into targetTable t1 using sourceTable t2 on (t1.id = abs(t2.id)) when matched then update set t1.sales = t2.sales when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);
select * from targetTable;

######## n(t) vs n(s) match #######
# OBE-30926: unable to get a stable set of rows in the source tables
--error 30926
merge into targetTable t1 using sourceTable t2 on (t1.id != t2.id) when matched then update set t1.sales = t2.sales when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);

####### where condition #######
truncate table targetTable;
truncate table sourceTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);
insert into targetTable values(3,3);

insert into sourceTable values(1,2);
insert into sourceTable values(2,4);
insert into sourceTable values(3,6);
insert into sourceTable values(4,8);

merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales where t2.id!= t1.id when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);
select * from targetTable;

##### update on clause related colunm #####
# OBE-38104: Columns referenced in the ON Clause cannot be updated: "T1"."ID"
--error 38104
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.id = 1;

##### update view use default value #####
# OBE-32575: Explicit column default is not supported for modifying views
## update/insert view not check default value, need add later.
##--error 600
merge into targetView t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = default;

###################### test for delete clause ###################
##### without delete condition #####
# OBE-00967: missing WHERE keyword
# 直接报语法错误
# --error 600
# merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales delete;

##### delete condition out of update condition #####
truncate table targetTable;
truncate table sourceTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);
insert into targetTable values(3,3);

insert into sourceTable values(1,2);
insert into sourceTable values(2,4);
insert into sourceTable values(3,6);
insert into sourceTable values(4,8);

merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales where t1.id < 3 delete where t1.id < 5 when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);
select * from targetTable;

##### origin value or new value use in delete condition #####
truncate table targetTable;
truncate table sourceTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);
insert into targetTable values(3,3);

insert into sourceTable values(1,2);
insert into sourceTable values(2,4);
insert into sourceTable values(3,6);
insert into sourceTable values(4,8);

merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when matched then update set t1.sales = t2.sales delete where t1.sales =2 when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales);
select * from targetTable;

###################### test for insert clause ###################
##### insert condition ######
truncate table targetTable;
truncate table sourceTable;

insert into targetTable values(1,1);
insert into targetTable values(2,2);
insert into targetTable values(3,3);

insert into sourceTable values(1,2);
insert into sourceTable values(2,4);
insert into sourceTable values(3,6);
insert into sourceTable values(4,8);

merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales) where t2.id > 0;
select * from targetTable;
# OBE-38102: Invalid column in the INSERT WHERE Clause: "T1"."ID"
--error 38101
merge into targetTable t1 using sourceTable t2 on (t1.id = t2.id) when not matched then insert(t1.id, t1.sales) values(t2.id, t2.sales) where t1.id > 0;

##### insert view with default #####
# OBE-32575: Explicit column default is not supported for modifying views
## update/insert view not check default value, need add later.
--error 32575 
merge into targetView t1 using sourceTable t2 on (t1.id = t2.id) when not matched then insert(t1.id, t1.sales) values(default, default);
