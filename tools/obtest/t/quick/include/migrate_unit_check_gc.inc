#############################################
#owner : jingyu.cr
#owner group: rs
#description: quick test
# 测试点：验证unit迁移后老unit被回收
# 需配合migrate_unit.inc使用
#############################################

connection conn_sys;
ob1.check_until_timeout SELECT COUNT(*) from oceanbase.GV\$OB_UNITS WHERE UNIT_ID = $unit_id and tenant_id = $mysql_tenant_id, 1, 100;
ob1.check_until_timeout SELECT COUNT(*) from oceanbase.GV\$OB_UNITS WHERE UNIT_ID = $unit_id and tenant_id = $mysql_tenant_id - 1, 1, 100;
