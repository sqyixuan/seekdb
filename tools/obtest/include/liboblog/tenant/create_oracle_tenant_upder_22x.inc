# create oracle tenant

ob1.mysql alter system set merger_switch_leader_duration_time = '10s';
ob1.mysql alter system set resource_hard_limit= 800;
sleep 2;
ob_connect(conn_sys, ob1.proxy0, root@sys, , oceanbase, , USE_MYSQL_CONNECTOR);

--disable_warnings
drop tenant if exists oracle;
--disable_warnings
drop resource pool if exists oracle_resource_pool;
--disable_warnings
drop resource unit if exists oracle_resouce_unit;

### create oracle tenant
ob1.mysql create resource unit oracle_resouce_unit max_cpu 2, max_memory 2073741824, max_iops 100000, max_disk_size '5G', max_session_num 64, MIN_CPU=1, MIN_MEMORY=2073741824, MIN_IOPS=128;
ob1.mysql create resource pool oracle_resource_pool unit = 'oracle_resouce_unit', unit_num = 1;
ob1.mysql create tenant oracle replica_num = 1, resource_pool_list=('oracle_resource_pool') set ob_tcp_invited_nodes='%', ob_compatibility_mode='oracle';
# oracle模式下autocommit值已为off,会引起诸多case出现因事务迟迟未提交导致的各种问题 为了规避此类问题,将mysqltest环境,obtest环境的该变量,在创建租户之后设置为on
ob1.mysql alter tenant oracle set variables autocommit='on';

sleep 2;
ob_connect(conn_oracle, ob1, SYS@oracle, ,SYS, , USE_OB_CONNECTOR);

### create user
create user obtest IDENTIFIED BY "obtest";
grant all on *.* to obtest WITH GRANT OPTION;
ob_connect(conn_oracle_obtest,ob1,OBTEST@oracle,obtest,obtest,,USE_OB_CONNECTOR);
