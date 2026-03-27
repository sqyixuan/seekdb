#=========================================================================================
#author: shouju.zyp
#本文件提供了检查归档是否完成的procedure，用于检查switchover 主切备后，检查主租户是否把所有日志归档
#使用方法示例：
#   call oceanbase.check_not_archive_ls_count(租户id);
set @@session.ob_query_timeout = 10000000;

use oceanbase;

#check_not_archive_ls_count: 检查还没有归档完成的LS数量
drop procedure if exists check_not_archive_ls_count;
delimiter //;
CREATE PROCEDURE check_not_archive_ls_count(check_tenant_id bigint)
BEGIN
  SELECT count(*)
  FROM GV$ob_log_stat AS primary_log_stat
  WHERE primary_log_stat.role!='LEADER'; END//

#end of procedure
delimiter ;//
