# mysql pl

echo "[CASE_BEGIN] [mysql_pl.inc]";
ob1.oblog.gen_conf tb_white_list=*cdc*.*.* log_level=DEBUG skip_ob_version_compat_check=1;
ob1.oblog.restart;

connection conn_obcdc_mysql;
set autocommit=1;
create database oblog_db1;
ob1.oblog.next;
use oblog_db1;

CREATE PROCEDURE my_proc() BEGIN END;
ob1.oblog.next;
DROP PROCEDURE my_proc;
ob1.oblog.next;

CREATE FUNCTION my_proc() RETURNS INT BEGIN RETURN 1; END;
ob1.oblog.next;
DROP FUNCTION my_proc;
ob1.oblog.next;

CREATE TABLE t(col INT);
ob1.oblog.next;
CREATE PROCEDURE my_proc() BEGIN INSERT INTO t VALUES(1); END;
ob1.oblog.next;
CALL my_proc();
ob1.oblog.next;
DROP PROCEDURE my_proc;
ob1.oblog.next;

# stop liboblog
ob1.oblog.stop;
echo "[CASE_END] [mysql_pl.inc]";
