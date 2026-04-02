
echo "[CASE_BEGIN] [oblog_compat_backward_common.inc]";

connection conn_obcdc_mysql;
create database cdc_mysql_db;
use cdc_mysql_db;
# 启动liboblog
ob1.oblog.gen_conf tb_white_list=cdc_mysql.cdc_mysql_db.* skip_ob_version_compat_check=1;
ob1.oblog.restart;

# 1. 准备表
# 验证DDL能否正常发现新增表
--disable_warnings
drop table if exists oblog_test1;
create table oblog_test1 (pk int primary key, c1 varchar(100), c2 datetime);

set ob_org_cluster_id=1234;

# 2. 验证単分区事务
set autocommit=1;
replace into oblog_test1 values (1,1,'2017-01-20 01:00:00'), (2,2,'2017-01-20 02:00:00'), (3,3,'2017-01-20 03:00:00');
ob1.oblog.next_dml;

set autocommit=0;
update oblog_test1 set c1='a', c2='2018-01-20 00:00:01' where pk=1;
update oblog_test1 set c1='b', c2='2019-01-20 00:00:02' where pk=2;
update oblog_test1 set c1='c', c2='2020-01-20 00:00:03' where pk=3;
commit;
ob1.oblog.next_dml;

# 停止oblog
ob1.oblog.stop;

echo "[CASE_END] [oblog_compat_backward_common.inc]";
