--disable_warnings
DROP TABLE IF EXISTS t1,t2,t3,t4,t5,t6,t7,t8,t_no_pk,np_pk;

# 1) partitioning keys are included in all unique keys (valid test)
eval CREATE TABLE t1 (
    col1 INT NOT NULL PRIMARY KEY,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    UNIQUE KEY (col1, col2, col3) LOCAL
)
PARTITION BY $partition_type(col1)
PARTITIONS 4;

eval CREATE TABLE t2 (
    col1 INT NOT NULL,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    primary key(col1, col3),
    UNIQUE KEY (col1, col3) LOCAL
)
PARTITION BY $partition_type(col1 , col3)
PARTITIONS 4;

eval CREATE TABLE t7 (
    col1 INT NOT NULL,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    PRIMARY KEY(col1, col2)
)
PARTITION BY $partition_type(col1 , col2)
PARTITIONS 4;

eval CREATE TABLE t8 (
    col1 INT NOT NULL,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    PRIMARY KEY(col1, col2, col4),
    UNIQUE KEY(col2, col1) LOCAL
)
PARTITION BY $partition_type(col1 , col2)
PARTITIONS 4;

# 2) Some partitioning keys are not included in some unique keys (invalid test)
DROP TABLE IF EXISTS t1,t2,t3,t4,t5,t6,t7,t8,t_no_pk,np_pk;
--error 1503
eval CREATE TABLE t1 (
    col1 INT NOT NULL PRIMARY KEY,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    UNIQUE KEY (col1, col2) LOCAL
)
PARTITION BY $partition_type(col3)
PARTITIONS 4;

--error 1503
eval CREATE TABLE t2 (
    col1 INT NOT NULL PRIMARY KEY,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    UNIQUE KEY (col1, col2) LOCAL,
    UNIQUE KEY (col3) LOCAL
)
PARTITION BY $partition_type(col1 , col3)
PARTITIONS 4;

CREATE TABLE t4 (
    col1 INT NOT NULL PRIMARY KEY,
    col2 INT NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    UNIQUE KEY (col1, col3) LOCAL,
    UNIQUE KEY (col2, col4) LOCAL
);

--error 1503
eval CREATE TABLE t5 (
    col1 INT NOT NULL,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    PRIMARY KEY(col1, col2)
)
PARTITION BY $partition_type(col3)
PARTITIONS 4;

--error 1503
eval CREATE TABLE t6 (
    col1 INT NOT NULL,
    col2 DATE NOT NULL,
    col3 INT NOT NULL,
    col4 INT NOT NULL,
    PRIMARY KEY(col1, col3),
    UNIQUE KEY(col2) LOCAL
)
PARTITION BY $partition_type( col2 )
PARTITIONS 4;
--enable_warnings
## 3) When there is non unique key, any column is valid in partitioning expression (valid test)
#eval CREATE TABLE t_no_pk (c1 INT, c2 INT)
#     PARTITION BY $partition_type(c1) PARTITIONS 4;
#ALTER TABLE t_no_pk ADD PRIMARY KEY(c1);
#ALTER TABLE t_no_pk DROP PRIMARY KEY;
#ALTER TABLE t_no_pk ADD PRIMARY KEY(c1, c2);
#ALTER TABLE t_no_pk DROP PRIMARY KEY;
##--error 1503
#ALTER TABLE t_no_pk ADD PRIMARY KEY(c2);
#
## 4) Change non-partitioned table to partitioned table using alter table
#CREATE TABLE np_pk (
#     id INT NOT NULL AUTO_INCREMENT,
#     name VARCHAR(50),
#     added DATE,
#     PRIMARY KEY (id)
#);
##--error 1503
#eval ALTER TABLE np_pk
#     PARTITION BY $partition_type( added )
#     PARTITIONS 4;
#eval ALTER TABLE np_pk
#     PARTITION BY $partition_type(id)
#     PARTITIONS 4;
#
#
