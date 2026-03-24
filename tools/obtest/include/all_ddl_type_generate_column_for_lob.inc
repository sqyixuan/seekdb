#owner: 修铭
#owner group: liboblog
#description: 测试liboblog能否支持LOB类型的生成列

let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (oblog_conn_sys,$ip,root@sys,,oceanbase,$port);

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.oblog_db1.*;
ob1.oblog.restart;

# sys租户下操作
connection oblog_conn_sys;

# 创建database oblog_db1
# 后续所有的操作都在这个DB下
create database oblog_db1;
use oblog_db1;

set ob_org_cluster_id=9876;

# LOB场景
create table t9(pk int primary key, tiny_text tinytext, tiny_blob tinyblob, text_type text, blob_type blob, medium_text mediumtext, medium_blob mediumblob, long_text longtext, long_blob longblob);

# LOB隐式生成列
create index prefix_idx on t9 (tiny_text(3));
insert into t9 values (1, 'tiny_text', 'tiny_blob', 'text_type', 'blob_type', 'medium_text', 'medium_blob', 'long_text', 'long_blob');
ob1.oblog.next_dml;

# LOB显示生成列
alter table t9 add column x mediumtext as (concat(medium_text, ".", medium_blob));
update t9 set pk = 2, medium_text='xy', medium_blob='za' where pk = 1;
ob1.oblog.next_dml;

# 停止liboblog
ob1.oblog.stop;

--disable_warnings
drop database if exists oblog_db1;
