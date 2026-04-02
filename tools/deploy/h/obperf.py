@ObServerBuilder
def ObServerL2(**__kw):
    # monitor related functions
    pidstat = '''!ssht: pidstat -ruwd -h -C observer 1'''
    dstat = '''!ssht: dstat -Neth0,eth1,bond0'''
    mpstat = '''!ssht: mpstat -P ALL 1'''
    iostat = '''!ssht: dstat -df'''
    top = '''!ssht: top'''
    # performance related functions
    perf_time = 30;
    perf_record = '''!ssht: cd $obdir && echo will record ${perf_time} seconds && sudo perf record -g -p ${pid} sleep ${perf_time} && sudo chmod a+r perf.data'''
    perf_report = '''!ssht: cd $obdir && perf report'''
    perf_stat = '''!ssht: echo will record ${perf_time} seconds; perf stat -p ${pid} sleep ${perf_time}'''
    perf = '''!seq: perf_record perf_report '''
    # obperf related
    obperf_record_start = '''!ssh: pkill -50 -u $usr -f '^$obdir/bin/' '''
    obperf_record_stop = '''!ssh: pkill -51 -u $usr -f '^$obdir/bin/' '''
    obperf_get_pfda = '''!call: get observer.pfda'''
    obperf_report = '''!ssht: cd $obdir && $server_start_environ tools/obperf observer.pfda $_rest_'''
    obperf_dump = '''!ssht: cd $obdir && $server_start_environ tools/obperf -D observer.pfda'''
    obperf_report_function = '''!ssht: cd $obdir && $server_start_environ tools/obperf -p observer.pfda'''
    sql_text = '!sh: mysql -h $ip -P $mysql_port -uroot -Doceanbase -e "select PLAN_ID, statement SQL_TEXT from oceanbase.__all_virtual_plan_stat\G" '
    atomic_perf_start = '''!ssh: pkill -43 -u $usr -f '^$obdir/bin/' '''
    atomic_perf_stop = '''!ssh: pkill -43 -u $usr -f '^$obdir/bin/' '''
    atomic_perf_report = '''!ssht: cd $obdir && $server_start_environ tools/obperf -s atomic_op.pfda'''
    atomic_perf_dump = '''!ssht: cd $obdir && $server_start_environ tools/obperf -D atomic_op.pfda | less'''
    # bianque related
    do_bianque_check = '''!ssh: which bianque >/dev/null'''
    bianque_check = '''!check: do_bianque_check # bianque not exist, plz install it(wget http://050719.oss-cn-hangzhou-zmf.aliyuncs.com/bianque_2_2_2-7_6u_generic.tar.gz) '''
    bianque_graph = '''!ssh: bqgdir=$bianque_data_dir/`date +%Y-%m-%d-%H-%M-%S`; mkdir -p $bqgdir; cd $bqgdir;bianque -p`$pid_cmd` --comm=$role -d $sysbench_max_time --drop=$bianque_drop_per $bianque_other_param'''
    bianque = '''!seq: bianque_check bianque_graph'''
    return build_dict(locals(), **__kw)
