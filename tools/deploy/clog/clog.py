def CLogServer():
    idx = 0
    ip='127.0.0.1'
    port=8045
    self_addr = '$ip:$port'
    start_arg = 'start $workdir'
    workdir='clog$idx'
    role = 'clog_server'
    less = 'sh: less $workdir/log'
    cat = 'sh: cat $workdir/log'
    cat_clog = 'sh: bin/log_tool read $workdir/clog/1'
    pid = """popen: pgrep -f -x '^$bin $start_arg' || echo 'noprocess'"""
    gdb = """sh: gdb --pid=`pgrep -f -x '^$bin $start_arg'`"""
    pstack = """sh: pstack `pgrep -f -x '^$bin $start_arg'`"""
    start = '''sh: ulimit -c unlimited
    ulimit -s 10240
    mkdir -p $data_dir/$idx/$usr/clog$idx
    mkdir -p $workdir
    ln -sf $data_dir/$idx/$usr/clog$idx $workdir/clog
    echo "server=$ip:$port" > $workdir/config
    pgrep -f -x '^$bin $start_arg' || $partition_leader_env $bin $start_arg 2>$workdir/stderr.log &'''.replace('\n', ';')
    stop = '''sh: pkill -9 -f -x '^$bin $start_arg.*' || echo 'no server$idx running' '''
    clean = 'sh: rm -rf $workdir && rm  -rf $data_dir/$idx/$usr/clog$idx'
    restart = 'seq: stop start'
    id = '$workdir@$ip:$port'
    return locals()

def Partition():
    role = 'partition'
    leader = '127.0.0.1:8045'
    create = 'sh: $admin add_partition $partition_idx $server_list $leader'
    id = 'partition$idx@$leader'
    write = 'sh: $admin submit_log $partition_idx 100 $leader'
    start_stress = 'sh: nohup $admin submit_log $partition_idx 1000000000 $leader >p$idx.log 2>&1 &'
    stop_stress = "sh: pkill -9 -f -x '^$admin submit_log $partition_idx.*'"
    add_server = 'sh: $admin add_server $partition_idx 127.0.0.1:8047 $leader'
    del_server = 'sh: $admin del_server $partition_idx 127.0.0.1:8047 $leader'
    add_partition = 'sh: $admin add_partition $n_idx $server_list $n_leader'
    del_partition = 'sh: $admin del_partition $n_idx $server_list'
    return locals()

def CLogCluster():
    data_dir = '$HOME/data'
    usr = os.getenv('USER')
    cwd = popen('pwd').strip()
    bin = '$cwd/bin/clog_server'
    admin = '$cwd/bin/clog_admin'
    top_dir = popen('readlink -f ../..').strip()
    update_local_bin = 'sh: make -j8 -C $top_dir/src/ && make clean -C $top_dir/unittest/clog/deploy && make clean -C $top_dir/tools/log_tool && make ob_server_main post_msg_main -j8 -C $top_dir/unittest/clog/deploy && make clean -C $top_dir/tools/log_tool && make log_tool -j8 -C $top_dir/tools/log_tool && mkdir -p bin && rsync $top_dir/unittest/clog/deploy/ob_server_main bin/clog_server && rsync $top_dir/unittest/clog/deploy/post_msg_main bin/clog_admin && rsync $top_dir/tools/log_tool/log_tool bin/log_tool'
    pid = 'all: .+server pid'
    stop = 'all: .+server stop'
    clean2 = 'sh: rm -f post_msg_main.log && rm -f p*.log'
    clean = 'all: .+server clean'
    start = 'all: .+server start'
    server_id = 'all: .+server id'
    partition_id = 'all: partition id'
    id = 'seq: server_id partition_id'
    restart = 'seq: stop start'
    create_partition = 'all: partition create'
    reboot = 'seq: stop_stress stop clean start sleep[sleep_time=15] create_partition'
    write = 'all: partition write'
    start_stress = 'sh: nohup $admin submit_log $partition_num 1000000000 $server_list >p.log 2>&1 &'
    stop_stress = "sh: pkill -9 -f -x '^$admin submit_log.*'"
    add_server = 'all: partition add_server'
    del_server = 'all: partition del_server'
    add_partition = 'all: partition add_partition'
    del_partition = 'all: partition del_partition'
    def stress(**cluster):
	for i in range(20000):
            call_(cluster, 'write')
    return locals()
partition_attr = dict_filter_out_special_attrs(Partition())
clog_server_attr = dict_filter_out_special_attrs(CLogServer())
clog_cluster_attr = dict_filter_out_special_attrs(CLogCluster())

def make_partition(**attr):
    return dict_updated(partition_attr, **attr)
def make_clog_server(**attr):
    return dict_updated(clog_server_attr, **attr)
def make_clog_cluster(hosts='', partitions='', **attrs):
    cluster_dict = copy.copy(attrs)
    for idx, (ip,port) in enumerate(re.findall('([.0-9]+):([0-9]+)', hosts)):
        cluster_dict['c%d'%(idx)] = make_clog_server(ip=ip, port=port, idx=idx+2)
    for idx, leader in enumerate(re.findall('([.0-9]+:[0-9]+)', partitions)):
        cluster_dict['p%d'%(idx)] = make_partition(leader=leader, idx=idx+2, partition_idx = idx+1)
    leader_list = ' '.join('p%d=%s'%(idx + 1, leader) for idx, leader in enumerate(re.findall('([.0-9]+:[0-9]+)', partitions)))
    return dict_updated(clog_cluster_attr, server_list=hosts, partition_leader_env=leader_list, **cluster_dict)
