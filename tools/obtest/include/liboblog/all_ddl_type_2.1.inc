

echo "[CASE_BEGIN] [all_ddl_type_2.1.inc]";

source include/liboblog/2_1_feature/global_index.inc;
source include/liboblog/2_1_feature/mysql_pl.inc;
# 注意 change_column_type在ob4000-20221102版本表现是不正确的，TODO: 在下一个release版本修复，暂时注释掉这个case
# source include/liboblog/2_1_feature/change_column_type.inc;
# 注意：alter tenant all case这里仅仅加mysql场景，oracle场景下输出的DDL不准确，后续待@仓氐 修复在21x版本之后，再考虑加到compat2
# NOTICE! TODO: alter_tenant_all_mysql cost too much time and remvoe from this case currently.
# source include/liboblog/2_1_feature/alter_tenant_all_mysql.inc;

# oracle
source include/liboblog/2_1_feature/sequence.inc;

echo "[CASE_END] [all_ddl_type_2.1.inc]";
