 #
 # owner:千溪
 # owner group:liboblog
 # description:
 # 测试点: 1.4.4支持的DDL语句
 #        1. 增加和删除分区
 #        2. 用户和权限DDL
 #
 ##

 # 1.4.4新增了添加分区和删除分区
 source include/all_ddl_type_add_or_drop_partition_upder_147.inc;

 # 1.4.4新增了迭代用户和权限DDL
 source include/all_ddl_type_user_and_grant.inc;
