connection conn_obcdc_mysql;

ob1.oblog.gen_conf tb_white_list=cdc_mysql.*.* log_level=DEBUG;
ob1.oblog.restart;

################################# ddl basic test ###############################

# 创建database 1
create database ddl_basic_db1;
# 输出create database DDL
ob1.oblog.next;

use ddl_basic_db1;

create database ddl_basic_db2;
ob1.oblog.next;

drop database ddl_basic_db2;
ob1.oblog.next;

create table ddl_basic_db1.ddl_t1 (pk int primary key);
ob1.oblog.next;

# 创建表t2
create table ddl_basic_db1.ddl_t2 (pk int primary key);
ob1.oblog.next;

# 创建带索引的表
create table if not exists ddl_basic_db1.t3 (c1 int primary key, c2 int, c3 int, c4 int, c5 int, key idx_1 (c1,c2,c3), key idx_2 (c2,c4), key idx_3 (c3,c2), unique key uk_test (c4,c2,c5),unique key uk_test1 (c2,c1));
ob1.oblog.next;

# index create and drp
DROP INDEX idx_2 on t3;
ob1.oblog.next;

# 逆序索引NOT_SUPPORT
CREATE INDEX idx_2a on t3(c4,c2);
ob1.oblog.next;

DROP INDEX uk_test on t3;
ob1.oblog.next;

CREATE INDEX uk_testa on t3(c4,c2);
ob1.oblog.next;

# 创建带生成列的表
create table table_with_generate_column(c1 int, c2 int, c3 int as (c1 + c2));
ob1.oblog.next;

insert into table_with_generate_column (c1, c2) VALUES(1, 2);
ob1.oblog.next;

insert into table_with_generate_column (c1, c2) VALUES(3, 4);
ob1.oblog.next;

drop table table_with_generate_column;
ob1.oblog.next;


# 在oblog_tt租户下, drop table ddl_t1
drop table ddl_basic_db1.ddl_t1;
ob1.oblog.next;


alter table ddl_basic_db1.ddl_t2  ADD COLUMN c1 int;
ob1.oblog.next;

alter table ddl_basic_db1.ddl_t2  ADD COLUMN c2 int;
ob1.oblog.next;

alter table ddl_basic_db1.ddl_t2 drop COLUMN c2;
ob1.oblog.next;

alter table ddl_basic_db1.ddl_t2  RENAME to ddl_t200;
ob1.oblog.next;

# rename table ddl_t2
RENAME table ddl_basic_db1.ddl_t200 to ddl_t300;
ob1.oblog.next;

insert into ddl_basic_db1.ddl_t300 VALUE(1, 1);
ob1.oblog.next;


######################################## dml basic test ################################

create database dml_basic_db1;
ob1.oblog.next;

use dml_basic_db1;

create table t_dml (id int primary key auto_increment, col1 varchar(10), col2 decimal(10,2));
ob1.oblog.next;

insert into t_dml(col1, col2) values ('aaaaa', 9.15);
ob1.oblog.next;

insert into t_dml(col1, col2) values ('bbbbb', 899.99);
ob1.oblog.next;

begin;
insert into t_dml(col1, col2) values ('ccccc', 333.33);
update t_dml set col2=300 where col1='ccccc';
commit;
ob1.oblog.next;


delete from t_dml where col1='aaaaa';
ob1.oblog.next;


ob1.oblog.stop;