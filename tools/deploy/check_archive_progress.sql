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
CREATE PROCEDURE check_not_archive_ls_count(check_tenant_id bigint) BEGIN 
  SELECT count(*)
  FROM cdb_ob_ls
  LEFT JOIN GV$ob_log_stat AS primary_log_stat
      ON cdb_ob_ls.tenant_id = primary_log_stat.tenant_id
          AND cdb_ob_ls.ls_id = primary_log_stat.ls_id
  LEFT JOIN 
      (SELECT orig.tenant_id AS tenant_id,
          orig.ls_id AS ls_id,
          orig.checkpoint_scn AS checkpoint_scn
      FROM __all_virtual_ls_log_archive_progress orig
      LEFT JOIN 
          (SELECT max(round_id) AS max_round_id,
          tenant_id,
          ls_id
          FROM __all_virtual_ls_log_archive_progress
          WHERE status= 'DOING'
          GROUP BY  tenant_id,ls_id) AS max_round_id_table
              ON max_round_id_table.max_round_id = orig.round_id
                  AND max_round_id_table.tenant_id = orig.tenant_id
                  AND max_round_id_table.ls_id = orig.ls_id
          LEFT JOIN 
              (SELECT max(piece_id) AS max_piece_id,
          tenant_id,
          ls_id
              FROM __all_virtual_ls_log_archive_progress
              WHERE status= 'DOING'
              GROUP BY  tenant_id,ls_id) AS max_piece_id_table
                  ON max_piece_id_table.max_piece_id = orig.piece_id
                      AND max_piece_id_table.tenant_id = orig.tenant_id
                      AND max_piece_id_table.ls_id = orig.ls_id
              WHERE status= 'DOING' ) AS archive_log_stat
              ON cdb_ob_ls.tenant_id = archive_log_stat.tenant_id
              AND cdb_ob_ls.ls_id = archive_log_stat.ls_id
  WHERE cdb_ob_ls.tenant_id = check_tenant_id
          AND primary_log_stat.role='LEADER'
          AND archive_log_stat.checkpoint_scn!=primary_log_stat.end_scn
          AND cdb_ob_ls.STATUS 
              NOT IN ('CREATING', 'CREATED', 'TENANT_DROPPING', 'CREATE_ABORT', 'PRE_TENANT_DROPPING'); END//

#end of procedure
delimiter ;//
