# 连接obtest租户
#let $ip=deploy_get_value(ob1.proxy0.ip);
#let $port=deploy_get_value(ob1.proxy0.port);
#connect (conn_obtest_obcdc,$ip,root@mysql,,*NO-ONE*,$port);
#connection conn_obtest_oblog;
connection conn_obtest;
use oblog_db;

ob1.oblog.exist;

set ob_org_cluster_id=1234;
replace into oblog_test values (1, 1), (2,2), (3,3);
ob1.oblog.next_dml;
inc $OBLOG_DML_COUNT;

ob1.oblog.stop;

# 从升级前回拉liboblog
# 启动liboblog
ob1.oblog.set_start_tstamp $start_tstamp_before_upgrade;
ob1.oblog.gen_conf tb_white_list=mysql.*.*;
ob1.oblog.restart;

let $cnt=0;

while ($cnt < $OBLOG_DML_COUNT)
{
  ob1.oblog.next_dml;
  inc $cnt;
}

ob1.oblog.stop;

ob1.oblog.set_start_tstamp $start_tstamp_before_upgrade;
ob1.oblog.gen_conf tb_white_list=*.*.* skip_ob_version_compat_check=1;
ob1.oblog.restart;

sleep 30;

ob1.oblog.stop;
# 删除数据表
drop table if exists oblog_test;
