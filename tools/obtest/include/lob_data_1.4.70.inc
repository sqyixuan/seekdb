let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 启动liboblog
# ouput md5sum
ob1.oblog.set_extra_argument -m;
# 关闭校验唯一ID模式
ob1.oblog.set_disable_verify_mode;
ob1.oblog.gen_conf tb_white_list=sys.oblog_db1.*;
ob1.oblog.restart;

connection oblog_conn_sys;

# 设置最大包大小 1G
set global max_allowed_packet=1073741824;

# 重新建立连接
# 让包大小设置生效
connect (oblog_conn_sys,$ip,admin@sys,admin,oceanbase,$port);

# sys租户下操作
connection oblog_conn_sys;

# 设置事务超时时间: 100秒
set ob_trx_timeout=100000000;

# 创建database 1
create database oblog_db1;
use oblog_db1;

create table t1(pk int primary key, tiny_text tinytext, tiny_blob tinyblob, text_type text, blob_type blob, medium_text mediumtext, medium_blob mediumblob, long_text longtext, long_blob longblob);

# 达到单列和单行限制
# 除了longtext和longblob外，其他列达到限制
insert into t1 values (3, repeat('1', 256), repeat('2', 256), repeat('3', 65536), repeat('4', 65536), repeat('5', 16777216), repeat('6', 16777216), "", "");
select md5(tiny_text),  md5(tiny_blob), md5(text_type), md5(blob_type), md5(medium_text), md5(medium_blob), md5(long_text), md5(long_blob) from t1 where pk = 3;
ob1.oblog.next_dml;

# longtext达到48M限制 + longblob 11M = 59M
# 注意：不能达到60M，还有其他列，总和加起来一定大于了60M
insert into t1 values (4, "", "", "", "", "", "", repeat('a', 50331648), repeat('b', 11534336));
select md5(long_text), md5(long_blob)  from t1 where pk = 4;
ob1.oblog.next_dml;

# longtext 11M + longblob 48M = 59M
insert into t1 values (5, "", "", "", "", "", "", repeat('c',11534336), repeat('d', 50331648));
select md5(long_text), md5(long_blob)  from t1 where pk = 5;
ob1.oblog.next_dml;

# longtext 11M + longblob 48M = 59M
# update 把longtext的值与longblob的值交换
update t1 set long_text=repeat('d', 50331648), long_blob=repeat('c', 11534336) where pk = 5;;
select md5(long_text), md5(long_blob) from t1 where pk = 5;
ob1.oblog.next_dml;

# update
# medium_text: 11M
# medium_blob: 16M
# long_text: 16M
# long_blob: 16M
update t1 set medium_text=repeat('e', 11534336), medium_blob=repeat('f', 16777216), long_text=repeat('g', 16777216), long_blob=repeat('h', 16777216) where pk=5;
select md5(medium_text), md5(medium_blob), md5(long_text), md5(long_blob) from t1 where pk = 5;
ob1.oblog.next_dml;

# 停止oblog
ob1.oblog.stop;

--disable_warnings
drop database if exists oblog_db1;
