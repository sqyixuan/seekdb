let $RANDOM_NUM=query_get_value(select abs(random()) ran from dual,ran,1);
let $CASE_TYPE_RAN=$CASE_TYPE_$RANDOM_NUM;

let $ODPS_CMD_CRETAB = odpscmd -e "use mysqltest_regression_sqlqa;CREATE TABLE IF NOT EXISTS ZCEXT1_$CASE_TYPE_RAN(c1 INT);CREATE TABLE IF NOT EXISTS t3_write_$CASE_TYPE_RAN(c1 TINYINT, c2 SMALLINT, c3 INT, c4 BIGINT, c5 FLOAT, c6 DOUBLE, c7 DECIMAL(38,18), c8 BOOLEAN, c9 STRING, c10 VARCHAR(100), c11 CHAR(100), c12 BINARY, c13 TIMESTAMP, c14 DATE, c15 TIMESTAMP_NTZ, c16 DATETIME);";
exec $ODPS_CMD_CRETAB;

let $ODPS_CMD = odpscmd -e "use mysqltest_regression_sqlqa;truncate table ZCEXT1_$CASE_TYPE_RAN;truncate table t3_write_$CASE_TYPE_RAN;";
exec $ODPS_CMD;


