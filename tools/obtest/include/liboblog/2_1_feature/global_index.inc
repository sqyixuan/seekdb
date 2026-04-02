#
# owner:千溪
# owner group:liboblog
# description:
# 测试点:
#  全局索引测试
#  OB_DDL_CREATE_GLOBAL_INDEX
#  OB_DDL_ALTER_GLOBAL_INDEX
#  OB_DDL_RENAME_GLOBAL_INDEX
#  OB_DDL_DROP_GLOBAL_INDEX
#  OB_DDL_MODIFY_GLOBAL_INDEX_STATUS
#
##

echo "[CASE_BEGIN] [global_index.inc]";

ob1.oblog.gen_conf tb_white_list=oblog_tt.oblog_db1.*|*cdc*.*.* sort_trans_participants=1 skip_ob_version_compat_check=1;
ob1.oblog.restart;

connection conn_obcdc_mysql;
set autocommit=1;
# 创建database
create database oblog_db1;
ob1.oblog.next;
use oblog_db1;

################################ 1 分区添加测试
# 全局索引建表
source include/liboblog/global_index/global_index_create_table.inc;

# stop liboblog
ob1.oblog.stop;

ob1.oblog.gen_conf tb_white_list=sys.*.*|oblog_tt.oblog_db1.*|*cdc*.*.* sort_trans_participants=1 skip_ob_version_compat_check=1;
ob1.oblog.restart;

set @@recyclebin = off;

################################ 2. 分区删除
################################ (1) 删除全局索引

# 删除全局普通索引
drop index g_normal_key_t3 on t3;
ob1.oblog.next;
drop index g_normal_key_t4 on t4;
ob1.oblog.next;
drop index g_normal_key_t5 on t5;
ob1.oblog.next;
drop index g_normal_key_t6 on t6;
ob1.oblog.next;

drop index g_unique_gkey_t7 on t7;
ob1.oblog.next;
drop index g_unique_gkey_t8 on t8;
ob1.oblog.next;
drop index g_unique_gkey_t9 on t9;
ob1.oblog.next;
drop index g_unique_gkey_t10 on t10;
ob1.oblog.next;

################################ (2) 删除主表

let $TABLE_NUMBER = 10;
let $i = 1;
while ($i <= $TABLE_NUMBER)
{
  eval drop table t$i;
  ob1.oblog.next;
  inc $i;
}

drop database oblog_db1;
ob1.oblog.next;

# stop liboblog
ob1.oblog.stop;

echo "[CASE_END] [global_index.inc]";
