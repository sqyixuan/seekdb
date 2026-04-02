# 同义词
#
# ACT(OB_DDL_CREATE_SYNONYM,)
# ACT(OB_DDL_DROP_SYNONYM,)
# ACT(OB_DDL_REPLACE_SYNONYM,)

echo "[CASE_BEGIN] [2_0_synonym.inc]";

connection conn_obcdc_mysql;
set autocommit=1;
--disable_warnings
drop database if exists synonym_db;
drop database if exists other_db;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=oblog_tt.synonym_db.*|oblog_tt.other_db.*|*cdc*.*.*;
ob1.oblog.restart;

create database synonym_db;
ob1.oblog.next;
create database other_db;
ob1.oblog.next;

use other_db;
create table t1(c1 varchar(20));
ob1.oblog.next;
insert into t1 values("in other_db");
ob1.oblog.next;
create view v1 as select * from t1;
ob1.oblog.next;

use synonym_db;
create table t1(c1 varchar(20));
ob1.oblog.next;
insert into t1 values("in synonym_db");
ob1.oblog.next;
create table t2(c1 int);
ob1.oblog.next;
insert into t2 values(999);
ob1.oblog.next;

## 1. OB_DDL_CREATE_SYNONYM
create synonym synonym_db.spn for synonym_db.t1;
ob1.oblog.next;
create synonym other_db.spn for other_db.t1;
ob1.oblog.next;
create synonym synonym_db.spn_other for other_db.t1;
ob1.oblog.next;
create synonym other_db.spn_syn for synonym_db.t1;
ob1.oblog.next;

# select * from synonym_db.spn;
# select * from other_db.spn;
# select * from synonym_db.spn_other;
# select * from other_db.spn_syn;

## 2. OB_DDL_DROP_SYNONYM
drop synonym other_db.spn;
ob1.oblog.next;
drop synonym other_db.spn_syn;
ob1.oblog.next;

## 3. OB_DDL_REPLACE_SYNONYM
create or replace synonym synonym_db.spn_other for other_db.t2;
ob1.oblog.next;
# select * from synonym_db.spn_other;

# 停止oblog
ob1.oblog.stop;

echo "[CASE_END] [2_0_synonym.inc]";
