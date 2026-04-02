let $ODPS_path = /data/1/wb-zhch01084078/odps/bin;

#let $ODPS_CMD = $ODPS_path/odpscmd -e "create table test(a int);insert into test values(1);select * from test;drop table test;";

##zcext1
let $ODPS_CMD_CREATE_ZCEXT1 = $ODPS_path/odpscmd -e "create table zcext1(c1 int);insert into zcext1 values (1),(2),(null);";

##t1
let $ODPS_CMD_CREATE_T1 = $ODPS_path/odpscmd -e "create table t1(c1 int,c2 int,c3 string,c4 DATE) ;INSERT INTO t1 VALUES(1,1,'a',NULL);";

##T2
let $ODPS_CMD_CREATE_T2 = $ODPS_path/odpscmd -e "create table t2(c1 TINYINT,c2 SMALLINT,c3 INT,c4 BIGINT,c5 FLOAT,c6 DOUBLE,c7 DECIMAL(10,2),c8 BOOLEAN,c9 STRING,c10 VARCHAR(100),c11 CHAR(100),c12 BINARY,c13 TIMESTAMP,c14 DATE,c15 TIMESTAMP_NTZ,c16 DATETIME) ;INSERT INTO t2 VALUES (1Y, 101S, 1001, 10001L, 10.1F, 20.2D, 123.456, TRUE, 'string1', 'varchar1', 'char1', CAST('binary1' AS BINARY), TIMESTAMP'2021-01-01 00:00:01', DATE'2021-01-01', TIMESTAMP_NTZ '2021-01-01 00:00:01', DATETIME'2021-01-01 00:00:01'),(-2Y, -102S, -1002, -10002L, -10.2F, -20.3D, -456.789, FALSE, 'string2', 'varchar2', 'char2', CAST('binary2' AS BINARY), TIMESTAMP'2021-01-02 00:00:02', DATE'2021-01-02', TIMESTAMP_NTZ'2021-01-02 00:00:02', DATETIME'2021-01-02 00:00:02'),(3Y, 103S, 1003, 10003L, 10.3F, 20.4D, 789.012, TRUE, 'string3', 'varchar3', 'char3', CAST('binary3' AS BINARY), TIMESTAMP'2021-01-03 00:00:03', DATE'2021-01-03', TIMESTAMP_NTZ'2021-01-03 00:00:03', DATETIME'2021-01-03 00:00:03' ),(-4Y, -104S, -1004, -10004L, -10.4F, -20.5D, -012.345, FALSE, 'string4', 'varchar4', 'char4', CAST('binary4' AS BINARY), TIMESTAMP'2021-01-04 00:00:04', DATE'2021-01-04', TIMESTAMP_NTZ'2021-01-04 00:00:04', DATETIME'2021-01-04 00:00:04'),(5Y, 105S, 1005, 10005L, 10.5F, 20.6D, 345.678, TRUE, 'string5', 'varchar5', 'char5', CAST('binary5' AS BINARY), TIMESTAMP'2021-01-05 00:00:05', DATE'2021-01-05', TIMESTAMP_NTZ'2021-01-05 00:00:05', DATETIME'2021-01-05 00:00:05');INSERT INTO t2 VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,NULL, NULL, NULL, NULL, NULL, NULL);INSERT INTO t2 VALUES (0Y, 0S, 0, 0, 0, 0, 0, FALSE, '', '','', CAST('' AS BINARY), TIMESTAMP'1970-01-01 00:00:00', DATE'1970-01-01', TIMESTAMP_NTZ'1970-01-01 00:00:00', DATETIME'1970-01-01 00:00:00');";

##T3
let $ODPS_CMD_CREATE_T3 = $ODPS_path/odpscmd -e "create table t3(c1 TINYINT,c2 SMALLINT,c3 INT,c4 BIGINT,c5 FLOAT,c6 DOUBLE,c7 DECIMAL,c8 BOOLEAN,c9 STRING,c10 VARCHAR(100),c11 CHAR(100),c12 BINARY,c13 TIMESTAMP,c14 DATE,c15 TIMESTAMP_NTZ,c16 DATETIME) ;insert into t3 values (1Y, 32767S, 2147483647, 9223372036854775807L, 3.402823466E+38F, 1.7976931348623157E+308, 123456.78, TRUE, '特殊字符 *&^% Aa', '特殊字符 *&^% Aa', 'A', CAST('Test' AS BINARY), TIMESTAMP'2022-01-01 00:00:00', DATE'2022-01-01', TIMESTAMP_NTZ'2022-01-01 12:00:00', DATETIME'2022-01-01 12:00:00') ;insert into t3 values (0Y, -32768S, -2147483647, -9223372036854775808L, -3.402823466E+38F, -1.7976931348623157E+308D, -123456.78, FALSE, '', NULL, 'b', CAST('test' AS BINARY), TIMESTAMP'2022-12-31 23:59:59', DATE'2022-12-31', TIMESTAMP_NTZ'2022-12-31 12:00:00', DATETIME'2022-12-31 12:00:00') ;insert into t3 values (127Y, 32767S, 2147483647, 9223372036854775807L, 3.402823466E+38F, 1.7976931348623157E+308D, 99999.99, TRUE, 'Null值测试', 'Null值测试', 'C', CAST('test' AS BINARY), TIMESTAMP'1970-01-01 00:00:00', DATE'1970-01-01', TIMESTAMP_NTZ'1970-01-01 12:00:00', DATETIME'1970-01-01 12:00:00') ;insert into t3 values (-128Y, -32768S, -2147483647, -9223372036854775808L, -3.402823466E+38F, -1.7976931348623157E+308D, -99999.99, FALSE, ' ', ' ', 'd', CAST('test' AS BINARY), TIMESTAMP'2038-01-19 03:14:07', DATE'2038-01-19', TIMESTAMP_NTZ'2038-01-19 12:00:00', DATETIME'2038-01-19 12:00:00') ;insert into t3 values (50Y, 100S, -100, 5000000000L, 3.1415926F, 2.718281828459045D, 12345.67, TRUE, '测试数据', '测试数据', 'E', CAST('test' AS BINARY), TIMESTAMP'2050-10-10 10:10:10', DATE'2050-10-10', TIMESTAMP_NTZ'2050-10-10 12:00:00', DATETIME'2050-10-10 12:00:00') ;insert into t3 values (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,NULL, NULL, NULL, NULL, NULL, NULL) ;insert into t3(C1,C2,C3,C4,C5,C6,C7,C9,C10,C11,C12) values (0Y,0S,0,0L,0F,0.0D,0.0,'','','',CAST('' AS BINARY)) ;";

##T4
let $ODPS_CMD_CREATE_T4 = $ODPS_path/odpscmd -e "clone table t3 to t4;";

##sale_detail
let $ODPS_CMD_CREATE_sale_detail = $ODPS_path/odpscmd -e "create table sale_detail(shop_name string,customer_id string,total_price double) partitioned by (sale_date string, region string);alter table sale_detail add partition (sale_date='2013', region='china') partition (sale_date='2014', region='shanghai');insert into sale_detail partition (sale_date='2013', region='china') values ('s1','c1',100.1),('s2','c2',100.2),('s3','c3',100.3);insert into sale_detail partition (sale_date='2014', region='shanghai') values ('null','c5',null),('s6','c6',100.4),('s7','c7',100.5);";

##sale_detail_clone
let $ODPS_CMD_CREATE_sale_detail_clone = $ODPS_path/odpscmd -e "clone table sale_detail partition (sale_date='2013', region='china') to sale_detail_clone if exists overwrite;";

##sale_detail_view
let $ODPS_CMD_CREATE_sale_detail_view = $ODPS_path/odpscmd -e "create view sale_detail_view (store_name, customer_id, price, sale_date, region) comment 'a view for table sale_detail' as select * from sale_detail;";

#exec $ODPS_CMD;
exec $ODPS_CMD_CREATE_ZCEXT1;
exec $ODPS_CMD_CREATE_T1;
exec $ODPS_CMD_CREATE_T2;
exec $ODPS_CMD_CREATE_T3;
exec $ODPS_CMD_CREATE_T4;
exec $ODPS_CMD_CREATE_sale_detail;
exec $ODPS_CMD_CREATE_sale_detail_clone;
exec $ODPS_CMD_CREATE_sale_detail_view;


