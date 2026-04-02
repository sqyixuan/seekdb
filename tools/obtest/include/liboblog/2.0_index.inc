# index
#
# OB_DDL_RENAME_INDEX 目前rename index记录的OB_DDL_ALTER_TABLE, orancle支持rename index, mysql不支持
# OB_DDL_DROP_INDEX_TO_RECYCLEBIN,
# OB_DDL_FLASHBACK_INDEX

echo "case: index begin";

source create_tenant.inc;

# 在oblog_tt租户下操作
connect (oblog_conn_tt,$ip,root@oblog_tt,,oceanbase,$port);
connection oblog_conn_tt;

# 清理数据
--disable_warnings
drop table if exists test;

# 启动liboblog
ob1.oblog.gen_conf tb_white_list=sys.synonym_db.*|sys.other_db.*;
ob1.oblog.restart;

set recyclebin=on;
create table test(c1 int primary key, c2 int, c3 int);
ob1.oblog.next;

create index test1_index on test(c2);
ob1.oblog.next;
create index test2_index on test(c3);
ob1.oblog.next;

# 1. OB_DDL_DROP_INDEX_TO_RECYCLEBIN,
drop index test1_index on test;
ob1.oblog.next;

# 2. OB_DDL_FLASHBACK_INDEX
# let $test1_index_recycle_name = query_get_value(select OBJECT_NAME FROM oceanbase.USER_RECYCLEBIN, OBJECT_NAME, 1);
let $test1_index_recycle_name= query_get_value(select object_name from oceanbase.__all_recyclebin order by gmt_create desc limit 1, object_name, 1);
echo $test1_index_recycle_name;
eval flashback index $test1_index_recycle_name to before drop;
ob1.oblog.next;

# purge
drop index test1_index on test;
ob1.oblog.next;
let $test1_index_recycle_name = query_get_value(select OBJECT_NAME FROM oceanbase.USER_RECYCLEBIN, OBJECT_NAME, 1);
echo $test1_index_recycle_name;
eval purge index $test1_index_recycle_name;
ob1.oblog.next;

# 3. OB_DDL_RENAME_INDEX
alter table test rename index test2_index to test2_index_copy;
ob1.oblog.next;

# 停止oblog
ob1.oblog.stop;
