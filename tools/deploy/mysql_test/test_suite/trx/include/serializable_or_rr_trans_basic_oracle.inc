#
# owner: dunqiao.gdq
# owner group: transaction
# description: The basic test cases of behaviors in SERIALIZABLE or REPEATABLE READ isolation level.
#              Oracle mode version.

--disable_query_log
set @@session.explicit_defaults_for_timestamp=off;
--enable_query_log

--error 0, 942
drop table t1;
--error 0, 942
drop table t2;

### SET TRANSACTION时确定事务的快照版本号
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;

connection conn2;
update t1 set c='aaa' where i=1;
commit;

connection conn1;
select * from t1;

rollback;

select * from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


### ALTER SESSION在第1条语句执行时确定事务的快照
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;

connection conn2;
update t1 set c='aaa' where i=1;
commit;

connection conn1;
select * from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


### 如果第1条语句不修改数据，该语句并不会做为一个独立的事务，后续语句使用相同的快照

--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1;

connection conn2;
update t1 set c='aaa' where i=1;
commit;
select * from t1;

connection conn1;
select * from t1;

rollback;

select * from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
update t1 set c='c' where i=3;

connection conn2;
update t1 set c='aaa' where i=1;
commit;

connection conn1;
select * from t1;

rollback;

select * from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


### 如果事务以SET TRANSACTION开启，则在该事务commit/rollback之前不允许执行SET TRANSACTION或者ALTER SESSION
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
select * from t1;
--error 1453
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
select * from t1;
--error 1453
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
rollback;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

### 如果事务不是以SET TRANSACTION开启的，并且该事务的前几条语句没有修改过数据，则允许在事务中执行SET TRANSACTION或者ALTER SESSION
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
update t1 set c='c' where i=3;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
rollback;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
rollback;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1 where i=3 for update;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
rollback;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
update t1 set c='aaa' where i=1;
--error 1453
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
rollback;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1 where i=1 for update;
--error 1453
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
rollback;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

### 执行set transaction或者alter session之后重新生成快照版本
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1;

connection conn2;
update t1 set c='aaa' where i=1;
commit;

connection conn1;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
select * from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1;

connection conn2;
update t1 set c='aaa' where i=1;
commit;

connection conn1;
select * from t1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

### 事务第一条语句失败后回滚整个事务，下条语句生成新快照、新事务。
### 非第一条语句报6235(OB_TRANS_CANNOT_SERIALIZE)后，后续语句会持续报6235。
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn2;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
rollback;

# case 1
connection conn1;
rollback;
update t1 set c='a111' where i=1;

connection conn2;
--source mysql_test/include/replace_object_name_created_by_sys.inc
--error 1
insert into t1 values (2, 'b');

connection conn1;
commit;

# 能够看到'a111'
connection conn2;
select * from t1;
rollback;

# case 2
connection conn1;
update t1 set c='a222' where i=1;

connection conn2;
--error 942
insert into t2 values (2, 'b');

connection conn1;
commit;

# 能够看到'a222'
connection conn2;
select * from t1;
rollback;

# case 3
connection conn2;
select * from t1;

connection conn1;
update t1 set c='a333' where i=1;
commit;

# 持续报6235
# 只能看到'a222'
connection conn2;
--error 8177
update t1 set c='a444' where i=1;
--error 8177
update t1 set c='a444' where i=1;
select * from t1;
rollback;

# case 4
connection conn1;
update t1 set c='a444' where i=1;

connection conn2;
delimiter //;
begin
  insert into t1 values (2, 'b');
  insert into t1 values (3, 'c');
  exception when others then
    null;
end;
//
delimiter ;//

connection conn1;
commit;

# 能够看到'a444'
connection conn2;
select * from t1;
rollback;

# case 5
connection conn1;
update t1 set c='a555' where i=1;

connection conn2;
delimiter //;
begin
  insert into t1 values (3, 'c');
  insert into t1 values (2, 'b');
  exception when others then
    null;
end;
//
delimiter ;//

connection conn1;
commit;

# 只能看到'a444'
connection conn2;
select * from t1;
rollback;

# case 6
connection conn1;
update t1 set c='a666' where i=1;

connection conn2;
delimiter //;
--source mysql_test/include/replace_object_name_created_by_sys.inc
--error 1
begin
  insert into t1 values (3, 'c');
  insert into t1 values (2, 'b');
end;
//
delimiter ;//

connection conn1;
commit;

# 能够看到'a666'
connection conn2;
select * from t1;
rollback;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

# Nonrepeatable read
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1 where i = 1;

connection conn2;
update t1 set c = 'aaa' where i = 1;
commit;

connection conn1;
select * from t1 where i = 1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
select * from t1 where i = 1;

connection conn2;
update t1 set c = 'aaa' where i = 1;
commit;

connection conn1;
select * from t1 where i = 1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

# Phantoms
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select count(*) from t1;

connection conn2;
delete from t1 where i = 1;
commit;

connection conn1;
select count(*) from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;


--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
select count(*) from t1;

connection conn2;
delete from t1 where i = 1;
commit;

connection conn1;
select count(*) from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

# tsc
--source mysql_test/test_suite/trx/include/serializable_trans_init.inc

connection conn1;
eval ALTER SESSION SET ISOLATION_LEVEL = $ser_or_rr;
select * from t1;

connection conn2;
update t1 set c = 'aaa' where i = 1;
commit;

connection conn1;
--error 8177
update t1 set c = '2aaa' where i = 1;
select * from t1;

DROP TABLE t1;
disconnect conn1;
disconnect conn2;

# pl tsc
connect (conn1,$OBMYSQL_MS0,$OBMYSQL_USR,$OBMYSQL_PWD,TEST,$OBMYSQL_PORT);
connect (conn2,$OBMYSQL_MS0,$OBMYSQL_USR,$OBMYSQL_PWD,TEST,$OBMYSQL_PORT);

connection conn1;
set autocommit=0;

# Initialize table.
--error 0, 942
DROP TABLE t;
--error 0, 942
DROP TABLE result;
CREATE TABLE t (col int);
CREATE TABLE result (col varchar(50));
INSERT INTO t VALUES (1);

COMMIT;

connection conn2;
set autocommit=0;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
SELECT * FROM t;

connection conn1;
eval SET TRANSACTION ISOLATION LEVEL $ser_or_rr;
UPDATE t SET col = 2;
COMMIT;

connection conn2;
DELIMITER //;

DECLARE
  not_serializable EXCEPTION;
  PRAGMA EXCEPTION_INIT(not_serializable,-6235);
BEGIN
  LOOP
    BEGIN
      UPDATE t SET col = 3;
      COMMIT;
      EXIT;
      EXCEPTION
        WHEN not_serializable THEN
        BEGIN
          ROLLBACK;
          INSERT INTO result VALUES('get exception');
        END;
    END;
  END LOOP;
END;
//
DELIMITER ;//

SELECT * FROM t;
SELECT * FROM result;

DROP TABLE t;
DROP TABLE result;
disconnect conn1;
disconnect conn2;
