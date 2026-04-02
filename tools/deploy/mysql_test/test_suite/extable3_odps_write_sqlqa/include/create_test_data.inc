SET ob_query_timeout=100000000000;
--disable_result_log
source mysql_test/test_suite/extable3_odps_write_sqlqa/include/trunc_odps_data.inc;
--enable_result_log
--error 0,1051
drop table zcext1;
--disable_query_log
--source mysql_test/test_suite/extable3_odps_write_sqlqa/include/replace_odps_info.inc
eval create external table zcext1 (c1 int )
PROPERTIES =(
type = "odps"
ACCESSID = "$ODPS_ACCESSID"
ACCESSKEY = "$ODPS_ACCESSKEY"
PROJECT_NAME = "mysqltest_regression_sqlqa"
ENDPOINT = "http://service.cn-hangzhou.maxcompute.aliyun.com/api"
TABLE_NAME ="ZCEXT1_$CASE_TYPE_RAN"
);
--enable_query_log
select * from zcext1;

--error 0,1051
drop table zcext1_data;
create table zcext1_data( c1 int );
insert into zcext1_data values(1),(2),(null);

insert into zcext1 select * from zcext1_data;
select * from zcext1;
insert overwrite zcext1 select * from zcext1_data;
select * from zcext1;

####其他数据类型
--error 0,1051
drop table zct2;
CREATE TABLE zct2(c1 TINYINT,c2 SMALLINT,c3 MEDIUMINT,c4 int,c5 BIGINT,c6 DECIMAL(5,2),c7 float(5,2),c8 double(5,2),c9 bit(6),c10 TINYINT(255) UNSIGNED,c11 SMALLINT(255) ZEROFILL,c12 MEDIUMINT(255) UNSIGNED,c13 int (255) UNSIGNED,c14 bigint(255) ZEROFILL,c15 decimal UNSIGNED,c16 float(5,2) UNSIGNED,c17 double(5,2) ZEROFILL,c18 datetime,c19 timestamp,c20 date,c21 time,c22 year,c23 char,c24 varchar(50),c25 binary,c26 varbinary(50),c27 TINYBLOB,c28 BLOB,c29 MEDIUMBLOB,c30 LONGBLOB,c31 TINYTEXT,c32 TEXT,c33 MEDIUMTEXT,c34 LONGTEXT,c35 ENUM('','a','1','中') CHARACTER SET utf16 ,c36 set('a', '1','','中') CHARACTER SET utf16,c37 json,c38 GEOMETRY);

INSERT INTO zct2 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38)
VALUES
(1, 32767, 8388607, 2147483647, 9223372036854775807, 123.45, 123.45, 123.45, b'101010', 255, 255, 255, 255, 9223372036854775807, 123, 123, 123, '2021-10-29 12:00:00', '2021-10-29 12:00:00', '2021-10-29', '12:00:00', 2021, 'a', 'varchar', b'1101001', b'1101001', 'tinyblob', 'blob', 'mediumblob', 'longblob', 'tinytext', 'text', 'mediumtext', 'longtext', 'a', 'a', '{"id": "1", "name": "Fred"}', ST_GeomFromText('POINT(1 1)'));

INSERT INTO zct2 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38)
VALUES
(-1, -32768, -8388608, -2147483648, -9223372036854775808, -123.45, -123.45, -123.45, b'101010', 255, 65535, 16777215, 4294967295, 18446744073709551615, 123.45, 123.45, 123.45, '2021-10-29 12:00:00', '2021-10-29 12:00:00', '2021-10-29', '12:00:00', 2021, 'a', 'varchar', b'1101001', b'1101001', 'tinyblob', 'blob', 'mediumblob', 'longblob', 'tinytext', 'text', 'mediumtext', 'longtext', 'a', 'a', '{}',ST_GeomFromText('GeometryCollection EMPTY'));

INSERT INTO zct2 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38)
VALUES
(0, 0, 0, 0, 0, 0.0, 0.0, 0.0, b'0', 0, 0, 0, 0, 0, 0.00, 0.00, 0.00, '1970-01-01 00:00:00', '1970-01-01 00:00:00', '1970-01-01', '00:00:00', 1970, '', '', b'', b'', '', '', '', '', '', '', '', '', '', '', '{}',ST_GeomFromText('GeometryCollection EMPTY'));

INSERT INTO zct2 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30, c31, c32, c33, c34, c35, c36, c37, c38)  VALUES  (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,null);


########other type

--error 0,1051
drop table zcext3;
--disable_query_log
--source mysql_test/test_suite/extable3_odps_write_sqlqa/include/replace_odps_info.inc
eval create external table zcext3 (
c1 TINYINT AS (external\$tablecol1),
c2 SMALLINT AS (external\$tablecol2),
c3 INT AS (external\$tablecol3),
c4 BIGINT AS (external\$tablecol4),
c5 FLOAT AS (external\$tablecol5),
c6 DOUBLE AS (external\$tablecol6),
c7 DECIMAL(38,18) AS (external\$tablecol7),
c8 BOOLEAN AS (external\$tablecol8),
c9 VARCHAR(100) AS (external\$tablecol9),
c10 VARCHAR(100) AS (external\$tablecol10),
c11 CHAR(100) AS (external\$tablecol11),
c12 VARBINARY(100) AS (external\$tablecol12),
c13 TIMESTAMP(6) AS (external\$tablecol13),
c14 DATE AS (external\$tablecol14),
c15 DATETIME(6) AS (external\$tablecol15),
c16 DATETIME(3) AS (external\$tablecol16)
)
PROPERTIES =(
type = "odps"
ACCESSID = "$ODPS_ACCESSID"
ACCESSKEY = "$ODPS_ACCESSKEY"
PROJECT_NAME = "mysqltest_regression_sqlqa"
ENDPOINT = "http://service.cn-hangzhou.maxcompute.aliyun.com/api"
TABLE_NAME ="t3_write_$CASE_TYPE_RAN"
);
--enable_query_log
