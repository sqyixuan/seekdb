####
#创建oracle租户用户
connection tentora_conn;
CREATE USER user1 IDENTIFIED BY password;
CREATE USER user2 IDENTIFIED BY password;
CREATE TABLEGROUP TABLEGROUP1;
eval CREATE TABLEGROUP TABLEGROUP2 SHARDING='PARTITION';
eval CREATE TABLEGROUP TABLEGROUP3 SHARDING='NONE';
CREATE TABLEGROUP TABLEGROUP1_1;
eval CREATE TABLEGROUP TABLEGROUP2_1 SHARDING='PARTITION';
eval CREATE TABLEGROUP TABLEGROUP3_1 SHARDING='NONE';
GRANT DBA TO user1;
GRANT DBA TO user2;
ob_connect (user1_conn,$proxyip,user1@$tent2name,password,,$proxyport);
CREATE TABLE t1_listpart(log_id INT,log_value VARCHAR2(20)) PARTITION BY LIST(log_value) (PARTITION P01 VALUES ('A'),PARTITION P02 VALUES ('B'),PARTITION P03 VALUES ('C'));
INSERT INTO t1_listpart (log_id, log_value) VALUES (1, 'A');
INSERT INTO t1_listpart (log_id, log_value) VALUES (2, 'B');
INSERT INTO t1_listpart (log_id, log_value) VALUES (3, 'C');
CREATE TABLE t1(employee_id NUMBER PRIMARY KEY, first_name VARCHAR2(50) NOT NULL, email VARCHAR2(100) NOT NULL UNIQUE, hire_date DATE NOT NULL);
INSERT INTO t1(employee_id, first_name, email, hire_date) VALUES(1, 'John', 'john@example.com', TO_DATE('2023-04-01', 'YYYY-MM-DD'));
ob_connect (user2_conn,$proxyip,user2@$tent2name,password,,$proxyport);
CREATE TABLE t2_listpart(log_id INT,log_value VARCHAR2(20)) PARTITION BY LIST(log_value) (PARTITION P01 VALUES ('A'),PARTITION P02 VALUES ('B'),PARTITION P03 VALUES ('C'));
INSERT INTO t2_listpart (log_id, log_value) VALUES (1, 'A');
INSERT INTO t2_listpart (log_id, log_value) VALUES (2, 'B');
INSERT INTO t2_listpart (log_id, log_value) VALUES (3, 'C');
CREATE TABLE t2(employee_id NUMBER PRIMARY KEY, first_name VARCHAR2(50) NOT NULL, email VARCHAR2(100) NOT NULL UNIQUE, hire_date DATE NOT NULL);
INSERT INTO t2(employee_id, first_name, email, hire_date) VALUES(1, 'John', 'john@example.com', TO_DATE('2023-04-01', 'YYYY-MM-DD'));

CREATE TABLEGROUP TABLEGROUP1_3;
eval CREATE TABLEGROUP TABLEGROUP2_3 SHARDING='PARTITION';
eval CREATE TABLEGROUP TABLEGROUP3_3 SHARDING='NONE';
ALTER TABLEGROUP TABLEGROUP1_3 ADD TABLE user1.t1_listpart;
ALTER TABLEGROUP TABLEGROUP2_3 ADD TABLE user1.t1;
ALTER TABLEGROUP TABLEGROUP3_3 ADD TABLE user1.t1;
