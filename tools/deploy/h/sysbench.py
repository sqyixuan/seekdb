__doc__ += '''
* sysbench
./hap.py ob1.sysbench [prepare|run|cleanup]
'''
@Call
def magic_xsh(d, cmd, args, kw):
    cmd_list = shlex.split(cmd)
    real_cmd = list(cmd_list) + list(args)
    if _dryrun_:
        return real_cmd
    return subprocess.call(real_cmd)

@Call
def magic_sbsh(d, cmd, args, kw):
    if (sysbench_ip == ''):
        return magic_sh(d, cmd, args, kw)
    else:
        return popen_wrapper(dict_updated(d, _rest_=args), 'ssh -T $usr@$sysbench_ip', input=cmd)

sql_user = 'root@tt1'
sysbench_bin = 'sysbench/sysbench'
sysbench_db = 'test'
sysbench_lua = 'sysbench/oltp.lua'
sysbench_max_time = 120
sysbench_table_size = 100000
sysbench_threads = 64
sysbench_report_interval = 10
sysbench_ip = ''
oltp_tables_count = 1

@ObServerBuilder
def ObServerMisc(**__kw):
    sqlslap = """!xsh: mysqlslap -h $ip -P $mysql_port -uadmin -padmin  --create-schema=test"""
    sysbench_init = """!sh: $rsync_cmd -a sysbench $usr@$sysbench_ip:~/"""
    #sysbench_run = """!sbsh: $sysbench_bin --mysql-db=$sysbench_db --test=$sysbench_lua --mysql-host=$ip --oltp-table-size=$sysbench_table_size --max-time=$sysbench_max_time --report-interval=$sysbench_report_interval --max-requests=0 --num-threads=$sysbench_threads --mysql-user=$sql_user --mysql-port=$mysql_port --db-ps-mode=disable --oltp-tables-count=$oltp_tables_count --oltp-dist-type=uniform   --oltp-point-selects=1 --oltp-simple-ranges=0 --oltp-sum-ranges=0 --oltp-order-ranges=0 --oltp-distinct-ranges=0 --oltp-skip-trx=on  --oltp-read-only=on run """
    sysbench_run = """!sbsh: $sysbench_bin --mysql-db=$sysbench_db --test=$sysbench_lua --mysql-host=$ip --oltp-table-size=$sysbench_table_size --max-time=$sysbench_max_time --report-interval=$sysbench_report_interval --max-requests=0 --num-threads=$sysbench_threads --mysql-user=$sql_user --mysql-port=$mysql_port --db-ps-mode=disable --oltp-tables-count=$oltp_tables_count run """
    sysbench_prepare = """!sbsh: $sysbench_bin --mysql-db=$sysbench_db --test=$sysbench_lua --mysql-host=$ip --oltp-table-size=$sysbench_table_size --mysql-user=$sql_user --mysql-port=$mysql_port --db-ps-mode=disable --oltp-tables-count=$oltp_tables_count prepare """
    sysbench_cleanup = """!sbsh: $sysbench_bin --mysql-db=$sysbench_db --test=$sysbench_lua --mysql-host=$ip --mysql-user=$sql_user --mysql-port=$mysql_port --db-ps-mode=disable --oltp-tables-count=$oltp_tables_count cleanup """
    sysbench_bianque = """!call: bianque"""
    def sysbench(*args, **kw):
        result = []
        if 'init' in args:
            result.append(['init', call(kw, 'sysbench_init')])
        if 'prepare' in args:
            result.append(['prepare', call(kw, 'sysbench_prepare')])
        if 'run' in args:
            result.append(['run', call(kw, 'sysbench_run')])
        if 'bianque' in args:
            result.append(['bianque', call(kw, 'sysbench_bianque')])
        if 'cleanup' in args:
            result.append(['cleanup', call(kw, 'sysbench_cleanup')])
        return result

    return build_dict(locals(), **__kw)

@ObInstanceBuilder
def ObInstanceMisc(**__kw):
    sbperf = '!call: rf s/sbperf.py'
    myperf = '!call: rf s/myperf.py'
    sqlslap = '!call: obs0.sqlslap'
    sysbench = '!call: obs0.sysbench'
    return build_dict(locals(), **__kw)
