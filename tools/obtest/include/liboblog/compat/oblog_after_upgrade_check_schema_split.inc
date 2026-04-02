# 连接obtest租户
let $ip=deploy_get_value(ob1.proxy0.ip);
let $port=deploy_get_value(ob1.proxy0.port);
connect (conn_obtest_oblog,$ip,root@obtest,,*NO-ONE*,$port);
connection conn_obtest_oblog;
use oblog_db;

######################## 验证起始位点在SCHEMA_SPLIT_FINISH前后场景
# 验证获取起始schema版本是否正确，是否正常设置拆分模式
# 首先在拆分之后执行DDL，下面验证这批数据是否拉到

create table oblog_test_schema_split (pk int primary key, c1 varchar(10), c2 int, unique key uk (c1));
# 一条数据
insert into oblog_test_schema_split values (1, 'a', 1), (2, 'aa', 2), (3, 'aaa', 3);

# 查询schema拆分schema version
connection conn_sys;
let $start_tstamp_before_split = query_get_value(select (schema_version - 1) as ver from __all_ddl_operation where operation_type=1506, ver, 1);
let $start_tstamp_after_split = query_get_value(select schema_version from __all_ddl_operation where operation_type=1506, schema_version, 1);

############ 1.0 从拆分之前启动
ob1.oblog.set_start_tstamp_usec $start_tstamp_before_split;
ob1.oblog.gen_conf tb_white_list=obtest.oblog_db.oblog_test_schema_split;
ob1.oblog.restart;

# 输出数据
ob1.oblog.next_dml;

ob1.oblog.stop;
# 确认liboblog初始化的是非拆分模式
ob1.check_log "init schema split mode(ret=0, ret=\"OB_SUCCESS\", is_schema_split_mode=false";

############ 1.0 从拆分之后启动
ob1.oblog.set_start_tstamp_usec $start_tstamp_after_split;
ob1.oblog.gen_conf tb_white_list=obtest.oblog_db.oblog_test_schema_split;
ob1.oblog.restart;

# 输出数据
ob1.oblog.next_dml;

ob1.oblog.stop;
# 确认liboblog初始化的是非拆分模式
ob1.check_log "init schema split mode(ret=0, ret=\"OB_SUCCESS\", is_schema_split_mode=true";
