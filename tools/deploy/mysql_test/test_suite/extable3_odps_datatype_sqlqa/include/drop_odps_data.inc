let $ODPS_CMD_DROP_DATA = $ODPS_path/odpscmd -e "DROP table zcext1;DROP table t1;DROP table t2;DROP table T3;DROP table T4;DROP TABLE sale_detail;DROP TABLE sale_detail_clone;DROP VIEW sale_detail_view;";

exec $ODPS_CMD_DROP_DATA;


