#
# owner:shuning.tsn
#
# description: 反复修改主键列,liboblog支持修改主键列row key可比

--disable_warnings
drop database if exists oblog_db;

create database oblog_db;
use oblog_db;

#############################################################################
# 多次修改数据类型，测试同一行row key冲突
# 观察所有dml串行执行
# 主键类型改变
create table tc1 (id tinyint, name varchar(5), v1 int, primary key(id, name));
insert into tc1 values (1, repeat('a', 5), 1);
sleep 1;

let $start_tstamp = query_get_value(select round(time_to_usec(now()) / 1000000) as now, now, 1);
sleep 2;
## 构造数据,修改主键类型,循环多次
let $total = 0;
let $cnt = 0;
while ($cnt < 2)
{
  inc $cnt;
  eval update tc1 set id = $cnt, name = repeat('a', $cnt) where v1 = 1;
  inc $total;
}

# 版本22x目前不支持下面DDL操作 报错ERROR 1235 (0A000): Not supported feature or function 20200907
#alter table tc1 modify id smallint;
let $cnt = 0;
while ($cnt < 2)
{
  inc $cnt;
  eval update tc1 set id = $cnt, name = repeat('b', $cnt) where v1 = 1;
  inc $total;
}

#alter table tc1 modify id mediumint;
let $cnt = 0;
while ($cnt < 2)
{
  inc $cnt;
  eval update tc1 set id = $cnt, name = repeat('c', $cnt) where v1 = 1;
  inc $total;
}

let $len = 10;
while ($len < 12)
{
  eval alter table tc1 modify name varchar($len);
  eval update tc1 set id = $len, name = repeat('d', $len) where v1 = 1;
  inc $len;
}


connection oblog_conn_sys;
use oceanbase;
let $tenant_id = query_get_value(select tenant_id from __all_tenant where tenant_name="oblog_tt", tenant_id, 1);
connection oblog_conn_tt;
# schema split mode, 在租户本身下面查询
use oceanbase;
let $max_ddl_schema_version = query_get_value(select max(schema_version) as ver from __all_ddl_operation, ver, 1);

connection oblog_conn_tt;
use oblog_db;
# 以构造dml为oblog同步数据开始时间,交替执行类型转换和数据更新
# 验证数据类型转换前后row key不变
ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db.* log_level=INFO data_start_schema_version=$tenant_id:$max_ddl_schema_version;
ob1.oblog.set_start_tstamp $start_tstamp;
ob1.oblog.restart;

# 启动oblog后，交替插入类型转换ddl和数据更新dml
#alter table tc1 modify id int;
let $cnt = 0;
while ($cnt < 2)
{
  inc $cnt;
  eval update tc1 set id = $cnt, name = repeat('e', $cnt) where v1 = 1;
  inc $total;
}

#alter table tc1 modify id bigint;
let $cnt = 0;
while ($cnt < 1)
{
  inc $cnt;
  eval update tc1 set id = $cnt, name = repeat('f', $cnt) where v1 = 1;
  inc $total;
}

let $cnt = 0;
while ($cnt < 5)
{
  inc $cnt;
  eval update tc1 set id = $cnt , name = repeat('g', $cnt) where v1 = 1;
  inc $total;
}

let $len = 30;
while ($len < 32)
{
  eval alter table tc1 modify name varchar($len);
  eval update tc1 set id = $len, name = repeat('h', $len) where v1 = 1;
  inc $len;
}
let $tmp_total = 0;
while ($tmp_total < $total)
{
  ob1.oblog.next_dml;
  inc $tmp_total;
}

ob1.oblog.stop;
