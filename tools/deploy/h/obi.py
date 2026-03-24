# -*- coding: UTF-8 *-*
__doc__  += '''
* OBI basic
./hap.py ob1.reboot
./hap.py ob1.restart
./hap.py ob1.sql
./hap.py ob1.id
./hap.py ob1.pid
./hap.py ob1.obs0.less
./hap.py ob1.obs0.gdb
./hap.py ob1.obs0.grep 'ERROR'
./hap.py ob1.obs0.kill -41
./hap.py ob1.ish '!all: all_observer kill_by_port'
'''
from math import fabs

import urllib
import urllib2
import stat
import os
import operator
IS_BUSINESS=True
def get_local_ip(**kw):
    return popen('hostname -i').strip().split('\n')[0]

ocp_config_server = 'http://ocp-cfg.alibaba.net:8080/services?User_ID=alibaba&UID=test'
role = 'none'
usr = os.getenv('USER')
uid = int(popen('id -u'))
home = os.path.expanduser('~')
data_dir = '$home/data'
local_dir = hap_file_path()
bianque_data_dir = '$home/bianque_data'
bianque_drop_per = '1.0%'
bianque_other_param = ''
local_ip = get_local_ip()
app_name='${ob_name}.$usr.$local_ip'
cluster_id = '1'
obdir = '$home/${ob_name}.$name'
# dev = 'bond0'
log_level = 'INFO'
obi_idx_gen = itertools.count(1)
port_gen = None
cluster_id_gen = itertools.count(1)
clog_dir = '$data_dir/1'
cgroup_dir = ''
server_start_environ = 'GCOV_PREFIX=$obdir GCOV_PREFIX_STRIP=2 LD_LIBRARY_PATH=./lib:/usr/local/lib:/usr/lib:/usr/lib64:/usr/local/lib64:$LD_LIBRARY_PATH LD_PRELOAD="$server_ld_preload" $environ_extras'
log_file = '$obdir/log/$role.log'
rslog_file = '$obdir/log/rootservice.log'
local_servers = '{observer,obproxy}'
src_dir = os.path.realpath(hap_file_path('../..'))
build_dir = '$src_dir'
init_help_sql_file = '$src_dir/src/sql/fill_help_tables-ob.sql'
init_sql_file = hap_file_path('./init.sql')
init_sql_file_ce =hap_file_path('./init_for_ce.sql')


init_user_sql_file = hap_file_path('./init_user.sql')
init_user_sql_file_oracle = hap_file_path('./init_user_oracle.sql')
init_sql_file_tpch = hap_file_path('./init_tpch.sql')
init_user_sql_file_tpch = hap_file_path('./init_user_oracle.sql')
mysql_cmd = './obclient --prompt "OceanBase(\u@\d)>"'
rsync_cmd = os.getenv("DEPLOY_RSYNC_CMD") or 'rsync -W '

sql_user = 'root'
obs_cfg = dict(datafile_disk_percentage='50',
               datafile_size='10G',
               log_disk_percentage='3',   # Default 10% CLOG disk usage
               memory_limit='15G',
               system_memory='5G',
               cpu_count='16',
               stack_size='512K',
               cache_wash_threshold='1G',
               __min_full_resource_pool_memory="1073741824",
               workers_per_cpu_quota='10',
               schema_history_expire_time='1d',
               net_thread_count='4'
               )
proxy_cfg = dict(syslog_level='DEBUG',
                 enable_metadb_used='false', # Do not use meta db
                 enable_qa_mode='TRUE',
                 location_expire_period='1',
                 obproxy_config_server_url='$proxy_cfg_url',
                 observer_sys_password='f0455d5edb4833fa2e91762e71b61bc35d45cf0b',
                )
ObServerBuilder = ChainBuilder()
ObInstanceBuilder = ChainBuilder()


def check_ob_version():
    cmd=os.popen('./bin/observer   --version 2>&1|grep -i \'OceanBase_CE 4\'')
    res=cmd.read()
    i = "OceanBase_CE 4" in res
    global IS_BUSINESS
    if operator.contains(res,"OceanBase_CE 4"):
        IS_BUSINESS=False
        print('obi check the ob version is CE')
    else:
        
        IS_BUSINESS = True
        print('obi check the ob version is business')
        

def get_match_child(d, pat):
    return dict((k, v) for k,v in d.items() if type(v) == dict and re.match(pat, v.get('role', '')))

def myquote(url):
    return url.replace('\n', '%0A')

def build_observer_start_args(**kw):
    start_args = '''-P $port -p $mysql_port -z $zone -n $app_name -c $cluster_id -d $actual_data_dir -l $log_level'''
    extra_args = []
    use_ipv6 = ':' in find(kw, 'ip')
    if use_ipv6:
        extra_args.append(' -6')
    if find(kw, 'dev'):
        extra_args.append(' -i $dev')
    cfg = find(globals(), 'obs_cfg') or {}
    cfg.update(find(kw, 'obs_cfg') or {})
    if find(kw, 'has_proxy') or find(kw, 'cluster_role'):
        cfg.update(obconfig_url='$cfg_url')
    else:
        extra_args.append("-r '$rootservice_list'")
    cfg_str = ",".join("%s='%s'"%(k, v) for (k, v) in cfg.items() if not k.endswith('__'))
    if cfg_str:
        extra_args.append('-o %s'%(cfg_str))
    return '%s %s'%(start_args, ' '.join(extra_args))

def build_obproxy_start_args(**kw):
    start_args = '''-p $mysql_port -n $app_name -o %s '''
    cfg = find(globals(), 'proxy_cfg') or {}
    cfg.update(find(kw, 'proxy_cfg') or {})
    if find(kw, 'mysql_list'):
        cfg['test_server_addr'] = '$mysql_list'
        cfg['server_routing_mode'] = 'mysql'
        cfg.pop('obproxy_config_server_url')
        start_args = '''-p $mysql_port -r $mysql_list -n $app_name -o %s'''
    cfg_str = ",".join("%s='%s'"%(k, v) for (k, v) in cfg.items() if not k.startswith('_'))
    return start_args % (cfg_str)

@ObServerBuilder
def ObServerL0(**__kw):
    conn_str = '$ip:$mysql_port'
    do_check_dir = '!ssh: mkdir -p $data_dir/3/ && touch $data_dir/3/mark.$usr'
    check_dir = '!check: do_check_dir # make sure you have write permission on $data_dir@$ip'
    data_dir_id = '$app_name.$name.$ob_name'
    observerdir = '$home/${ob_name}.$name'
    mkdir = '''!ssh: mkdir -p $obdir/store
    mkdir -p $obdir/log
    mkdir -p $data_dir/3/$data_dir_id/sstable
    mkdir -p $clog_dir/$data_dir_id/clog
    mkdir -p $data_dir/3/$data_dir_id/slog
    mkdir -p $data_dir/3/$data_dir_id/tslog
    if [ -n "$cgroup_dir" ]
    then mkdir -p $cgroup_dir/cpu/oceanbase/${ob_name}.$name
    ln -sf $cgroup_dir/cpu/oceanbase/${ob_name}.$name $obdir/cgroup
    fi
    ln -sf $clog_dir/$data_dir_id/clog $obdir/store/clog
    ln -sf $data_dir/3/$data_dir_id/slog $obdir/store/slog
    ln -sf $data_dir/3/$data_dir_id/tslog $obdir/store/tslog
    ln -sf $data_dir/3/$data_dir_id/sstable $obdir/store/sstable'''.replace('\n', '; ')
    rmdir = '''!ssh: rm -rf $clog_dir/$data_dir_id/ $data_dir_id/ $data_dir/3/$data_dir_id $obdir/{etc,admin,.conf,store,log,cgroup,wallet} # $obdir/core.*'''
    rmlogdir = '''!ssh: rm -rf $obdir/log # $obdir/core.*'''
    # observer_extend_cmd example
    rsync = '''!sh: $rsync_cmd -a bin lib tools etc admin plugin_dir wallet $usr@[$ip]:$obdir/'''
    rsync_local = '''!sh: cp -rf bin lib tools etc admin plugin_dir wallet $obdir/ || echo some dirs missed'''
    tail = '''!ssh: tail -f $log_file'''
    tailwf = '''!ssh: tail -f $log_file.wf'''
    grep = '''!ssh: grep $_rest_ $log_file'''
    ob_admin = '''!sh: tools/ob_admin -h $ip -p $port $_rest_'''
    id = '$obdir:$svr_type@$ip:$port:$mysql_port:$zone'
    exe = '$obdir/bin/$role'
    restart = '''seq: stop rsync start'''
    start_observer_cmd = '''$start_observer_cmd_prefix$exe'''
    start = '''!obstart_ssh: cd $obdir; ulimit -s 4096; ulimit -n 100000; ulimit -c unlimited; pgrep -u $usr -f '^$exe' || $server_start_environ $start_observer_cmd $start_${role}_args # DiscardXOutput'''
    def start_observer_args(*args, **kw):
        return build_observer_start_args(**kw)
    def start_obproxy_args(*args, **kw):
        return build_obproxy_start_args(**kw)
    ssh = '!sh: ssh $usr@$ip -t "cd $obdir/log;bash --login"'
    pid_cmd = "pgrep -u $usr -f ^$obdir/bin/observer"
    gdb = '''!ssht: cd $obdir; LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH gdb --pid=`$pid_cmd` '''
    latest_core = '''!ssh: cd ${obdir} && ls -t "core.*" |head -n1'''
    gdb_core = '''!ssht: cd ${obdir}; LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH gdb core.`cat run/observer.pid` '''
    dooba = '!sh: python2 $src_dir/script/dooba/dooba -h $ip -P $mysql_port -uroot@sys '
    api_dooba = '!sh: dooba -h $ip -P $mysql_port -uroot@sys '
    kill_by_port = '''!ssh: /sbin/fuser -n tcp $port -k $_rest_'''
    kill_by_name = '''!ssh: pkill $_rest_ -u $usr -f '^$obdir/bin/' '''
    kill = '!call: kill_by_name'
    try_stop = '''!ssh: if pkill -9 -u $usr -f '^$obdir/bin/'; then exit 1; fi  '''
    restart = '!seq: stop start'
    stop = '!tryloop[timeout=10,interval=0.1]: try_stop'
    ls = '''!ssht: cd ${obdir} && ls -lat $_rest_'''
    less = '''!ssht: less $log_file'''
    rslog = '''!ssht: less $rslog_file'''
    lblog = '''!ssht: grep 'RS.LB' $rslog_file | less'''
    cat = '''!ssh: cat $log_file'''
    put = '''!sh: scp -r $_rest_ $usr@$ip:~/'''
    get = '''!sh: scp -r $usr@$ip:$obdir/$_rest_ ./'''
    pid = '''!popen: ssh $usr@$ip 'pgrep -u $usr -f "^$obdir/bin/observer"' # NoException'''
    pid_local = '''!popen: pgrep -u $usr -f "^$obdir/bin/observer" # NoException'''
    bootstrap = '''!sql: alter system bootstrap $bootstrap_param; # ExceptionOnFail DiscardOutput'''
    add_server = '''!sql: alter system add server '$ip:$port' zone '$zone';'''
    sql = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uroot -Doceanbase -c $_rest_'
    mysql = '!call: sql'
    obmysql = '!call: sql'
    tt1_sql = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uroot@tt1 -Dtest -c $_rest_'
    sys_execute = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uroot -Doceanbase -e "$_rest_"'
    tt1_execute = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uroot@tt1 -Dtest -e "$_rest_"'
    oracle_execute = '!sh: $mysql_cmd  -h $ip -P $mysql_port -utest@oracle -DTEST -ptest -e "$_rest_"'
    oracle = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uSYS@oracle -DSYS -c $_rest_'
    mysql = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uroot@mysql -Dtest -c $_rest_'
    conn_oracle = '!sh: $mysql_cmd  -h $ip -P $mysql_port -utest@oracle -DTEST -ptest -c $_rest_'
    conn_mysql = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uadmin@mysql -Dtest -padmin -c $_rest_'
    sql_cmd = '$mysql_cmd  -h $ip -P $mysql_port -uroot -Doceanbase -c '
    conn_lbacsys = '!sh: $mysql_cmd  -h $ip -P $mysql_port -uLBACSYS@oracle -DLBACSYS -pLBACSYS -c $_rest_'
    conn_tpch = '!sh: $mysql_cmd  -h $ip -P $mysql_port -utpch@tpch -Dtpch -ptpch -c $_rest_'
    host_id = '$usr@$ip'
    addr = '$ip:$port'
    wait_service = "!sql: use mysql; select 'wait_service_ok'; /* DiscardOutput */"
    check_bootstrap = "!sql: use mysql; select 'check bootstrap ok' from dual; /* DiscardOutput */ "
    collect_log = '''!sh: mkdir -p $collected_log_dir/$run_id && ssh $usr@$ip sync && rsync $usr@$ip:$obdir/log/$role*.log "$collected_log_dir/$run_id/$role.log.$ip:$port" && rsync $usr@$ip:$obdir/log/election.log "$collected_log_dir/$run_id/election.log.$ip:$port" && rsync $usr@$ip:$obdir/log/rootservice.log "$collected_log_dir/$run_id/rootservice.log.$ip:$port"&& rsync $usr@$ip:$obdir/log/trace.log "$collected_log_dir/$run_id/trace.log.$ip:$port" &&  rsync $usr@$ip:$obdir/run/observer.pid "$collected_log_dir/$run_id/observer.pid.$ip:$port"  # NoException DiscardOutput'''
    collect_log_local = '''!sh: mkdir -p $collected_log_dir/$run_id/$role.log.$ip:$port && sync && mv $obdir/core.* "$collected_log_dir/$run_id/$role.log.$ip:$port"; cp -r $obdir/log/$role*.log "$collected_log_dir/$run_id/$role.log.$ip:$port" && cp $obdir/log/election.log "$collected_log_dir/$run_id/$role.log.$ip:$port" && cp $obdir/log/rootservice.log "$collected_log_dir/$run_id/$role.log.$ip:$port" && cp $obdir/log/trace.log "$collected_log_dir/$run_id/$role.log.$ip:$port" &&  rsync $usr@$ip:$obdir/run/observer.pid "$collected_log_dir/$run_id/observer.pid.$ip:$port"  # NoException DiscardOutput'''
    collect_log_all = '''!sh: mkdir -p $collected_log_dir/$run_id/$ip:$port && ssh $usr@$ip sync && rsync $usr@$ip:$obdir/log/* "$collected_log_dir/$run_id/$ip:$port/" &&  rsync $usr@$ip:$obdir/run/observer.pid "$collected_log_dir/$run_id/observer.pid.$ip:$port"  # NoException DiscardOutput'''
    collect_observer_log_all = '''!sh: mkdir -p $collected_log_dir/$run_id/$ip:$port && ssh $usr@$ip sync && rsync $usr@$ip:$obdir/log/observer.log "$collected_log_dir/$run_id/$ip:$port/" # NoException DiscardOutput'''
    collect_log_all_local = '''!sh: mkdir -p $collected_log_dir/$run_id/$ip:$port && sync && cp -r $obdir/log/* "$collected_log_dir/$run_id/$ip:$port" # NoException DiscardOutput'''
    pid_stack = '''!ssh: cd $obdir/log; pstack ${pid} > ${pid}.$ip:$port.stack.`date +%s`'''
    pid_stack_local = '''!sh: cd $obdir/log; pstack ${pid_local} > ${pid_local}.$ip:$port.stack.`date +%s`'''
    pid_ob_stack = '''!ssh: kill -60 ${pid}'''
    pid_ob_stack_local = '''!sh: kill -60 ${pid_local}'''
    grep_bs_log = '''!seq: collect_log_all'''
    try_terminate = '''!ssh: if pkill -u $usr -f '^$obdir/bin/'; then exit 1; fi  '''
    terminate = '!tryloop[timeout=100,interval=1]: try_terminate'
    test_lua = '''!sh: cat $src_dir/src/diagnose/lua/test.lua | ssh $usr@$ip "nc -U ${obdir}/run/lua.sock" > /dev/null'''
    local_test_lua = '''!sh: cat $src_dir/src/diagnose/lua/test.lua | nc -U ${obdir}/run/lua.sock > /dev/null'''
    return build_dict(locals(), **__kw)

@ObInstanceBuilder
def ObInstanceL0(**__kw):

    role = 'obi'
    do_check_local_bin = '!sh: ls $local_dir/bin/$local_servers >/dev/null'
    check_local_bin = '!check: do_check_local_bin # local bin file check failed. make sure "bin" "lib" "tools" dir and files exists!'
    all_host = '!uniq: all_server host_id '
    def check_ssh(timeout=1, **ob):
        ip_list = call(ob, 'all_host', _log_=0)
        dev = find(ob, 'dev')
        def format_ip(ip): return '-6 {}%{}'.format(ip, dev) if ip.startswith('fe80:') and ip.count(':') > 1 else ip
        result = [(ip, Async(check_ssh_connection, format_ip(ip), timeout)) for ip in ip_list]
        fail_list = [ip for (ip, async) in result if async.get(2) != 0]
        if fail_list:
            raise Fail('check ssh fail, make sure you can ssh to these machine without passwd', fail_list)
        return 'OK'
    check_dir = '!all: all_server check_dir'

    all_server = '!filter: is_match role ^ob'
    all_proxy = '!filter: is_match role .*proxy'
    all_observer = '!filter: is_match role .*server'
    all_obs = '!filter: is_match svr_type obs'
    all_mysql = '!filter: is_match svr_type mysql'

    update_local_bin = '!seq: build cpbin wget_obproxy cp_sys_package'
    #update_local_bin = '!seq: build cpbin'
    update_local_tools = '!seq: build_tools cp_tools'
    build = '!sh: cd $build_dir; ob-make observer/observer -C src'
    build_tools = '!sh: cd $build_dir; ob-make -j32 -k -C tools/log_tool; ./ob-make -k -C tools/ob_admin'
    #bin_list = '{src/observer/observer,src/obproxy/obproxy}'
    bin_list = 'src/observer/observer'
    #bin_list_using_dlink = '{src/observer/.libs/observer,src/obproxy/.libs/obproxy}'
    bin_list_using_dlink = 'src/observer/.libs/observer'
    tool_list = '{tools/log_tool/logtool,tools/ob_admin/ob_admin}'
    tool_list_using_dlink = '{tools/log_tool/.libs/logtool,tools/ob_admin/.libs/ob_admin}'
    lib_list = 'src/observer/.libs/libobserver*'
    cpbin = '''!sh:  mkdir -p bin lib tools
    if file $build_dir/src/observer/observer |grep shell
    then rsync -a $build_dir/$bin_list_using_dlink bin/ && rsync -a $build_dir/$lib_list lib/
    else  rsync -a $build_dir/$bin_list bin/
    fi'''.replace('\n', ';')
    wget_obproxy = '!sh: wget http://11.166.86.153:8877/obproxy -O bin/obproxy > wget.log 2>&1 && chmod 755 bin/obproxy'
    cp_tools = '''!sh: mkdir -p tools
    if file $build_dir/src/observer/observer |grep shell
    then rsync -a $build_dir/$tool_list_using_dlink tools/
    else  rsync -a $build_dir/$tool_list tools
    fi'''.replace('\n', ';')
    cp_sys_package = '''!sh: mkdir -p admin
    cp $build_dir/src/share/inner_table/sys_package/*.sql admin/'''.replace('\n', ';')

    # obi_extend_cmd example
    dooba = '!call: obs0.dooba'
    sql = '!call: obs0.sql'
    obmysql = '!call: sql'
    tt1_execute = '!call: obs0.tt1_execute'
    sys_execute = '!call: obs0.sys_execute'
    oraclemode = '!call: obs0.oracle'
    mysqlmode = '!call: obs0.mysql'
    connoracle = '!call: conn_oracle'
    connmysql = '!call: conn_mysql'
    connlbacsys = '!call: conn_lbacsys'
    conntpch = '!call: conn_tpch'
    id = '!all: all_server id'
    rsync = '!all: all_server rsync'
    mk_local_dir = '!sh: mkdir -p tools lib wallet'
    mkdir = '!all: all_server mkdir'
    cleanup = '!all: all_server rmdir'
    cleanlog = '!all: all_server rmlogdir'
    start_servers = '!all: all_observer start'
    grep_bs_logs = '!all: all_observer grep_bs_log'

    self_id = '$ob_name'
    start_time = get_ts_str()
    run_id = '$self_id.$start_time'

    collected_log_dir = '$local_dir/collected_log'
    rm_log_dir = '!sh: rm $collected_log_dir -rf'
    collect_log ='!all: all_server collect_log'
    collect_log_local ='!all: all_server collect_log_local'
    collect_log_all ='!all: all_server collect_log_all'
    collect_observer_log_all ='!all: all_server collect_observer_log_all'
    collect_log_all_local ='!all: all_server collect_log_all_local'
    pid_stack = '!all: all_server pid_stack'
    pid_stack_local = '!all: all_server pid_stack_local'

    grep_function_map = '''!sh: perl check_bootstrap_fail.pl  $collected_log_dir < function.map'''
    # ################## Offline OB's own Config URL configuration ####################
    # cfg_key = '$app_name.rslist'
    # cfg_url = 'http://10.244.4.22:8080/oceanbase_configer/v2/$cfg_key'
    #
    # proxy_cfg_key = '$app_name.proxy'
    # proxy_cfg_name = '$app_name'
    # proxy_cfg_url_common = 'http://10.244.4.22:8080/diamond/cgi/a.py?key=$proxy_cfg_key'
    # prepare_proxy_cfg = '''!sh: curl '$proxy_cfg_url_common&method=set&value=$proxy_cfg_content' > /dev/null 2>&1 '''
    # proxy_cfg_url = '$proxy_cfg_url_common&method=get'
    # ################## Offline OB's own Config URL configuration ####################
    # ################# Offline OCP Config URL Configuration #################
    cfg_url = '${ocp_config_server}&Action=ObRootServiceInfo&ObCluster=$app_name'
    proxy_cfg_url = '${ocp_config_server}&Action=GetObProxyConfig&ObRegionGroup=$app_name'
    # Clear cluster URL content command
    cleanup_config_url_content = '''!popen: curl -X POST '${ocp_config_server}&Action=DeleteObRootServiceInfoByClusterName&ClusterName=$app_name' '''
    # Register cluster information to Config URL command
    register_to_config_url = '''!popen: curl -X POST '${ocp_config_server}&Action=ObRootServiceRegister&ObCluster=$app_name&ObClusterId=$cluster_id'  '''
    prepare_proxy_cfg = ''
    # ################# Offline OCP Config URL Configuration #################

    start_proxy = '!all: all_proxy start'

    restart = '!seq: stop rsync start'
    start = '!seq:start_servers start_proxy'
    check_bootstrap_fail = '!seq: rm_log_dir collect_observer_log_all grep_function_map'
    stop = '!all: all_server stop'
    force_stop = '!call: stop _log_=0'
    terminate = '!all: all_server terminate'
    pid = '!all: all_server pid'
    pid_local = '!all: all_server pid_local'
    bootstrap = '!tryloop[timeout=120,interval=3]: obs0.bootstrap'
    wait_service = '!tryloop: obs0.wait_service'
    exec_help_init_sql = '!call: obs0.sql <$init_help_sql_file'
    add_server = '!all: all_obs add_server'


       
    exec_init_user_sql1 = '!call: obs0.oracle < $init_user_sql_file_oracle'
    exec_init_user_sql2 = '!call: obs0.mysql < $init_user_sql_file'
    exec_init_sql_tpch = '!call: obs0.sql < $init_sql_file_tpch'
    exec_init_user_sql_tpch = '!call: obs0.oracle < $init_user_sql_file_tpch'
    check_lua_diagnose = '!call: obs0.test_lua'
    local_check_lua_diagnose = '!call: obs0.local_test_lua'
    exec_init_sql = '!call: obs0.sql < $init_sql_file'        
    if not IS_BUSINESS:
        exec_init_sql = '!call: obs0.sql < $init_sql_file_ce'
        exec_init_user_sql1=''
        exec_init_user_sql_tpch=''
    
        
    tpch_reboot = '!seq: check_local_bin check_ssh force_stop sleep[sleep_time=3] cleanup check_dir mk_local_dir mkdir rsync prepare_proxy_cfg mkdir_for_gcda start_servers bootstrap add_server wait_service exec_init_sql_tpch exec_help_init_sql exec_init_user_sql_tpch  start_proxy check_bootstrap check_lua_diagnose'
    local_reboot = '!seq: force_stop sleep[sleep_time=3] cleanup mk_local_dir mkdir cp_sys_package local_cp_admin local_rsync_all prepare_proxy_cfg start_servers bootstrap add_server wait_service exec_init_sql exec_init_user_sql1 exec_init_user_sql2 start_proxy check_bootstrap local_check_lua_diagnose'
    ssh_reboot = '!seq: check_local_bin check_ssh force_stop sleep[sleep_time=3] cleanup check_dir mk_local_dir mkdir rsync prepare_proxy_cfg mkdir_for_gcda start_servers bootstrap add_server wait_service exec_init_sql exec_help_init_sql exec_init_user_sql1 exec_init_user_sql2 start_proxy check_bootstrap check_lua_diagnose'

    ssh_reboot_standby = '!seq: check_local_bin check_ssh force_stop sleep[sleep_time=3] cleanup check_dir mk_local_dir mkdir rsync prepare_proxy_cfg mkdir_for_gcda start_servers bootstrap add_server wait_service check_bootstrap register_to_primary check_lua_diagnose'
    # Register to the main database, execute ADD CLUSTER on the main database
    register_to_primary = '''!sh: echo "alter system add cluster '$app_name' cluster_id=$cluster_id" | $primary_sql_cmd'''
    # Access the primary database to execute SQL commands
    primary_sql_cmd = '!call: $primary_cluster.cluster_sql_cmd'
    # Execute sql_cmd through cluster
    cluster_sql_cmd = '!call: obs0.sql_cmd'

    cp_admin_obdir = '!sh: cp -rf admin $obdir'
    local_rsync_all = '!all: all_server rsync_local'
    local_cp_admin = '!all: all_server cp_admin_obdir'
     # mysql mode related
    mysql_mode_reboot = '!seq: force_stop cleanup rsync start_proxy check_bootstrap'

    def pre_reboot(*kw, **ob):
        is_multi_cluster = find(ob, 'is_multi_cluster');
        cluster_role = find(ob, 'cluster_role');
        # Before reboot, first clear the Config URL content
        # In the primary-standby database scenario, only clean up before the primary database reboot
        if is_multi_cluster != 1 or (is_multi_cluster == 1 and cluster_role == 'primary'):
          call(ob, 'cleanup_config_url_content');
        # Register cluster information to Config URL
        return call(ob, 'register_to_config_url')

    def reboot(*kw, **ob):
        def set_normal_config():
            init_config = ObCfg.get('init_config', '')
            for _ in ["cpu_count", "stack_size", "cache_wash_threshold"]:
              if _ not in init_config:
                 del obs_cfg[_]
            if "system_memory" not in init_config:
              obs_cfg["system_memory"] = '16G'
        def parse_size(size):
            bytes = 0
            if size.isdigit():
                bytes = int(size)
            else:
                units = {"B": 1, "K": 1<<10, "M": 1<<20, "G": 1<<30, "T": 1<<40}
                match = re.match(r'([1-9][0-9]*)([B,K,M,G,T])', size)
                bytes = int(match.group(1)) * units[match.group(2)]
            return bytes
        memory_limit = obs_cfg["memory_limit"]
        is_mini = memory_limit is not '' and parse_size(memory_limit) < (16<<30)
        # collection of operations before reboot
        call(ob, 'pre_reboot')
        if 'is_local' in ob and ob['is_local']:
            if is_mini:
                return call(ob, 'local_reboot')
            else:
                set_normal_config();
                return call(ob, 'local_reboot')
        elif 'mysql_list' in ob and len(ob['mysql_list']) > 0:
            return call(ob, 'mysql_mode_reboot')
        elif find(ob, 'cluster_role') == 'standby':
            return call(ob, 'ssh_reboot_standby')
        else:
            if is_mini:
                return call(ob, 'ssh_reboot')
            else:
                set_normal_config();
                return call(ob, 'ssh_reboot')

    check_bootstrap = '!call: obs0.check_bootstrap'
    obi_idx = obi_idx_gen.next()
    name = 'ob%d'%(obi_idx)
    def prepare_for_ish(kw):
        globals().update(obi=kw)
    def after_load(kw):
        kw.update(ob_name=kw.get('__name__', 'obx'))
    return build_dict(locals(), **__kw)

def parse_server_spec(spec):
    if not spec:
        raise Fail('server_spec is NULL')
    if spec.find("@") == -1:
        spec = "[%s]@test" % spec
    def get_all_server_list(spec):
        for svr_type, ip_port, zone, region, ofs in re.findall('([a-z]*)@?([0-9a-fA-F.:]+)@([a-zA-Z0-9-]*)@?([a-zA-Z0-9-]*)%?([a-zA-Z0-9.:]*)', ','.join(expand(spec))):
            if ip_port.count(':') >= 2 and '.' in ip_port:
                ip, port, mysql_port = re.split(':', ip_port)
            else:
                ip, port, mysql_port = ip_port, '', ''
            yield svr_type, ip, port, mysql_port, zone, region, ofs
    if  any(svr_type == 'rs' for svr_type, ip, port, mysql_port, zone, region, ofs in get_all_server_list(spec)):
        default_svr_type = 'obs'
    else:
        default_svr_type = 'rs'
    default_region = 'sys_region'
    all_server_list = [(svr_type or default_svr_type, ip, port, mysql_port, zone, region or default_region, ofs) for svr_type, ip, port, mysql_port, zone, region, ofs in get_all_server_list(spec)]
    return all_server_list


class NameFactory:
    def __init__(self):
        self.idx = dict(obproxy=itertools.count(0), observer=itertools.count(0), mysql=itertools.count(0))
        self.name = dict(obproxy='proxy', observer='obs', mysql='mysql')
    def get_name(self, role):
        if role not in self.name:
            raise Fail('unsupported role: %s'%(role))
        return '%s%d'%(self.name[role], self.idx[role].next())

def OBI(server_spec, **kw):
    global port_gen
    if not port_gen:
        port_gen = itertools.count(100 * (int(uid) % 500) + 10000)

    all_server_list = parse_server_spec(server_spec)
    logger.debug('all_server_list: %s'%(all_server_list))
    obi = ObInstanceBuilder.build()
    role_map = dict(rs='observer', obs='observer', proxy='obproxy', mysql='mysql')
    name_factory = NameFactory()
    has_proxy = any(svr[0] == 'proxy' for svr in all_server_list)
    for svr_type, ip, port, mysql_port, zone, region, ofs in all_server_list:
        role = role_map[svr_type]
        port = port and int(port) or port_gen.next()
        mysql_port = mysql_port and int(mysql_port) or (3306 if role == 'mysql' else port_gen.next())
        name = name_factory.get_name(role)
        if (ofs == ''):
            actual_data_dir = "$obdir/store"
        else:
            actual_data_dir = "ofs_addr://" + ofs
        logger.debug('add server: %s: %s %s %s %s'%(name, svr_type, ip, zone, region))
        obi[name] = ObServerBuilder.build(name=name, role=role, ip=ip, zone=zone, region=region,svr_type=svr_type, port=port, mysql_port=mysql_port, actual_data_dir=actual_data_dir)
    if not get_match_child(obi, '^.*server$') and not get_match_child(obi, 'mysql'):
        raise Fail('obi: not even one server defined')

    obi = dict_merge(obi, kw)
    def format_ip(ip): return '[{}]'.format(ip) if ':' in ip else ip
    rootservice_list='%s'%(';'.join('%s:%d:%d'%(format_ip(d['ip']), d['port'], d['mysql_port']) for d in get_match_child(obi, 'observer').values() if d['svr_type'] == 'rs'))
    # List of RS: only inner port: svr_ip:port;svr_ip:port
    rs_server_list='%s'%(','.join('%s:%d'%(format_ip(d['ip']), d['port']) for d in get_match_child(obi, 'observer').values() if d['svr_type'] == 'rs'))
    # obproxy can connect mysql for performace test
    mysql_list='%s'%(';'.join('%s:%d'%(format_ip(d['ip']), d['mysql_port']) for d in get_match_child(obi, 'mysql').values() if d['svr_type'] == 'mysql'))
    bootstrap_param = ''
    if obi.get('cluster_role'):
        bootstrap_param += 'CLUSTER %s '%(obi.get('cluster_role'))
    bootstrap_param += ','.join("REGION '%s' ZONE '%s' SERVER '%s:%d'"%(d['region'], d['zone'], format_ip(d['ip']), d['port']) for d in get_match_child(obi, 'observer').values() if d['svr_type'] == 'rs')
    if obi.get('cluster_role') == 'standby':
	    bootstrap_param += ' USER test PASSWORD "test"'
    return dict_updated(obi, rootservice_list=rootservice_list, rs_server_list=rs_server_list, bootstrap_param=bootstrap_param, basic_bootstrap_param=bootstrap_param, has_proxy=has_proxy, mysql_list=mysql_list)

def genconf(tpl_file, target_file, env):
    tpl_file, target_file = sub(tpl_file, env), sub(target_file, env)
    try:
        tpl = open(tpl_file).read()
    except IOError as e:
        raise Fail('read %s fail'%(tpl_file))
    try:
        open(target_file, 'w').write(sub(tpl, env))
    except IOError as e:
        raise Fail('write %s fail'%(target_file))
    return target_file
