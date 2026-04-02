 #
 # owner:千溪
 # owner group:liboblog
 # description:
 # 测试点: 1.4.5 alter zone相关ddl语句
 #
 ##

echo "[CASE_BEGIN] [all_ddl_type_1.4.50.inc]";

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;

--disable_warnings
--disable_query_log
--disable_result_log
sleep 5;
--error 0,1064
alter tenant cdc_mysql set variables ob_tcp_invited_nodes='%';
--enable_warnings
--enable_query_log
--enable_result_log

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.*.*|oblog*.*.*|*cdc*.*.* skip_ob_version_compat_check=1;
ob1.oblog.restart;

# 1.验证sys_ddl操作之前插入数据是否正常
# 在cdc_mysql租户下操作
connect (oblog_conn_tt,$ip,root@cdc_mysql,,oceanbase,$port);
connection oblog_conn_tt;
set autocommit=1;

# 创建database 1
--disable_warnings
drop database if exists oblog_db1;
--enable_warnings
create database oblog_db1;
ob1.oblog.next;

use oblog_db1;
# 创建表t1
create table oblog_db1.t1 (pk int primary key);
ob1.oblog.next;

# 插入一条数据
insert into oblog_db1.t1 values(1);
ob1.oblog.next;

# 验证生成列
create table t2(c1 varchar(10) primary key, c2 varchar(8) generated always as (substring(c1, 2)) virtual, c3 int) partition by key(c2) partitions 5;
ob1.oblog.next;

# 2.测试add/delete/start/stop/alter zone,迭代出空语句
# sys租户下操作
connection oblog_conn_sys;

# OB_DDL_ADD_ZONE
alter system add zone 'z4';

# OB_DDL_START_ZONE
alter system start zone 'z4';

# OB_DDL_ALTER_ZONE
alter system modify zone 'z4' idc 'aa';

# OB_DDL_STOP_ZONE
alter system stop zone 'z4';

# OB_DDL_DELETE_ZONE
alter system delete zone 'z4';

# 3.验证sys_ddl操作过后插入数据是否正常
connection oblog_conn_tt;
# 插入一条数据
insert into oblog_db1.t1 values(2);
ob1.oblog.next;

drop database oblog_db1;
ob1.oblog.next;
# 停止oblog
ob1.oblog.stop;

echo "[CASE_END] [all_ddl_type_1.4.50.inc]";
