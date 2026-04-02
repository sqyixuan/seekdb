ct_dir = '$home/${ob_name}.$ct_name'
def Client(**__kw):
    id = '$client@$ip:$ct_dir'
    role = 'client'
    ssh = '''!sh: ssh -t $ip $_rest_'''
    client_script = '$obdir/$client/stress.sh'
    rsync = '''!sh: rsync -az $client $usr@$ip:$dir'''
    start = '''!ssh: "${client_env_vars} client_idx=$idx $client_script start ${client_start_args}"'''
    clear = '''!ssh: $client_script clear"'''
    cleanup = '''!ssh: $client_script cleanup'''
    tail = '''!ssh: $client_script tail"'''
    stop = '!ssh: $client_script stop'
    check = '!ssh: $client_script check'
    reboot = 'seq: stop rsync clear start'
    collect_log = '''!sh: mkdir -p $collected_log_dir/$run_id && scp $usr@$ip:$dir/$client/*.log $collected_log_dir/$run_id/ '''
    return build_dict(locals(), **__kw)

def ClientSet(**__kw):
    all_client = '!filter: is_match role client'
    id = '!all: all_client id'
    role = 'clientset'
    check_local_file = '!all: all_client check_local_file'
    conf = '!all: all_client conf'
    prepare = '!all: all_client prepare'
    rsync = '!all: all_client rsync'
    start = '!all: all_client start'
    stop = '!all: all_client stop'
    clear = '!all: all_client clear'
    check = '!all: all_client check'
    collect_log = '!all: all_client collect_log'
    reboot = '!seq: check_local_file rsync stop clear conf prepare start'
    return build_dict(locals(), **__kw)


def CT(client='', client_spec='', **attr):
    if not os.path.exists(client):
        raise Fail('no client dir exists', client)
    try:
        client_vars = load_file_vars('%s/main.py'%(client))
    except Exception as e:
        raise Fail('load client %s fail'%(client), e)
    hosts = re.findall('[.0-9]+', ' '.join(expand(client_spec)))
    if not hosts:
        raise Fail('no client hosts defined', client_spec)
    ct = ClientSet(client=client, hosts=hosts)
    for idx, ip in enumerate(hosts):
        name = 'c%d'%(idx)
        ct[name] = Client(ip=ip, idx=idx, ct_name=name, **client_vars)
    return ct
