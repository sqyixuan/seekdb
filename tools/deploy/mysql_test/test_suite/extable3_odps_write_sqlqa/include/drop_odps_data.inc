let $ODPS_CMD_DROPTAB = odpscmd -e "use mysqltest_regression_sqlqa;DROP TABLE IF EXISTS ZCEXT1_$CASE_TYPE_RAN;DROP TABLE IF EXISTS t3_write_$CASE_TYPE_RAN;";
exec $ODPS_CMD_DROPTAB;
