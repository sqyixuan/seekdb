#
# Author: 千溪
# Note:   2.3 统一入口
#

echo "[CASE_BEGIN] [all_ddl_type_2.3.inc]";

# oracle
source include/liboblog/2_2_3_feature/security_audit.inc;

source include/liboblog/2_2_3_feature/tablespace.inc;
source include/liboblog/2_2_3_feature/trigger.inc;

echo "[CASE_END] [all_ddl_type_2.3.inc]";
