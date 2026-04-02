if (sysbench_ip == ''):
    !sh: if [ ! -x sysbench/sysbench ] ; then rm -f sysbench/sysbench; wget http://11.166.86.153:8877/sysbench -O sysbench/sysbench; chmod +x sysbench/sysbench; fi;

load('s/utility.py')
reboot_if_not_ok
cur_sql_conn = make_conn('obi.obs0')
set autocommit=1 #(type='sql')
cpu_num = 2
zone1 = `obi.obs0.zone`
if `select * from __all_tenant where tenant_name='tt1'`:
    print 'tenant tt1 already exist'
else:
    print 'create tenant tt1'
    alter system set resource_hard_limit = 300;
    drop tenant if exists tt1;
    drop resource pool if exists pool_large;
    drop resource unit if exists box;
    create resource unit box max_cpu 2, memory_size 1073741824;
    create resource pool pool_large unit = 'box', unit_num = 1, zone_list = ('$zone1');
    create tenant tt1 replica_num = 1,primary_zone='$zone1', resource_pool_list=('pool_large') set ob_tcp_invited_nodes='%';
    alter system set large_query_threshold = '900ms';
    alter system set enable_record_trace_log = false;
    sleep 3

lua_script='lua/insert.lua'
thread_num = 500
max_time = 300
sysbench_args = '--db-ps-mode=disable ---oltp_auto_inc=on --mysql-user=root@tt1 --mysql-db=test --test=$lua_script --mysql-host=${obi.obs0.ip} --mysql-port=${obi.obs0.mysql_port} --oltp-tables-count=1 --oltp_table_size=1000000 --num-threads=$thread_num'
sysbench_run_args = '--max-time=${max_time} --report-interval=1 --max-requests=0 --mysql-ignore-errors=6002,6001,4012,1213,1020,1205,4038 --oltp-dist-type=uniform'
$sysbench_bin $sysbench_args cleanup
$sysbench_bin $sysbench_args prepare
$sysbench_bin $sysbench_args $sysbench_run_args run
