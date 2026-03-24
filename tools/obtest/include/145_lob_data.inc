# 145 lob本质上是varchar
# 测试兼容性
#
# 大小限制
# tiny_text text_type medium_text long_text
# 256        64K       256K         512K

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 连接sys租户
connection oblog_conn_sys;
use test;

# 启动liboblog
ob1.oblog.set_extra_argument -m;
ob1.oblog.gen_conf tb_white_list=sys.test.*;
ob1.oblog.restart;

# 设置事务超时时间: 100秒
set ob_trx_timeout=100000000;

create table t1(pk int primary key, tiny_text tinytext, tiny_blob tinyblob, text_type text, blob_type blob, medium_text mediumtext, medium_blob mediumblob);
ob1.oblog.next;
create table t2(pk int primary key, long_text longtext, long_blob longblob);
ob1.oblog.next;

# NULL
insert into t1 values (1, NULL, NULL, NULL, NULL, NULL, NULL);
select md5(tiny_text), md5(tiny_blob), md5(text_type), md5(blob_type), md5(medium_text), md5(medium_blob) from t1 where pk=1;
select length(tiny_text), length(tiny_blob), length(text_type), length(blob_type), length(medium_text), length(medium_blob) from t1 where pk=1;
ob1.oblog.next_dml;
insert into t2 values (1, NULL, NULL);
select md5(long_text), md5(long_blob) from t2 where pk=1;
select length(long_text), length(long_blob) from t2 where pk=1;
ob1.oblog.next_dml;

# empty
insert into t1 values (2, "", "", "", "", "", "");
select md5(tiny_text), md5(tiny_blob), md5(text_type), md5(blob_type), md5(medium_text), md5(medium_blob) from t1 where pk=2;
select length(tiny_text), length(tiny_blob), length(text_type), length(blob_type), length(medium_text), length(medium_blob) from t1 where pk=2;
ob1.oblog.next_dml;
insert into t2 values (2, "", "");
select md5(long_text), md5(long_blob) from t2 where pk=2;
select length(long_text), length(long_blob) from t2 where pk=2;
ob1.oblog.next_dml;

insert into t1 values (3, repeat('1', 256), repeat('2', 256), repeat('3', 65536), repeat('4', 65536), repeat('5', 262144), repeat('6', 262144));
select md5(tiny_text), md5(tiny_blob), md5(text_type), md5(blob_type), md5(medium_text), md5(medium_blob) from t1 where pk=3;
select length(tiny_text), length(tiny_blob), length(text_type), length(blob_type), length(medium_text), length(medium_blob) from t1 where pk=3;
ob1.oblog.next_dml;
insert into t2 values (3, repeat('5', 524288), repeat('6', 524288));
select md5(long_text), md5(long_blob) from t2 where pk=3;
select length(long_text), length(long_blob) from t2 where pk=3;
ob1.oblog.next_dml;

# 停止oblog
ob1.oblog.stop;
