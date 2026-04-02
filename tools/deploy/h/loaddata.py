__doc__ += '''
* loaddata
./hap.py ob1.stream_loaddata from=/src/csv/data/dir parallel=30 servers=10
./hap.py ob1.net_loaddata scale=tpch_1g
'''

@Call
def magic_xsh(d, cmd, args, kw):
    cmd_list = shlex.split(cmd)
    real_cmd = list(cmd_list) + list(args)
    if _dryrun_:
        return real_cmd
    return subprocess.call(real_cmd)

@ObServerBuilder
def ObServerLoaddata(**__kw):
    copy_tpch_data = '''!sbsh: rsync -ruPpv $from/* $ip:$to'''
    init_load_data_file = '''!sbsh: cat /dev/null > loaddata.sql.tmp'''
    write_load_data_file = '''!sbsh: echo "$sql_str" >> loaddata.sql.tmp'''

    tpch_schema = hap_file_path('./tpch.sql')
    load_tpch_schema = '!call: conn_oracle < $tpch_schema'
    tpch_data = hap_file_path('./loaddata.sql.tmp')
    load_tpch_data = '''!sbsh: cat $tpch_data | xargs -d ";" -I CMD -P $parallel obclient -h $ip -P $mysql_port -utest@oracle -DTEST -ptest -c -e CMD'''

    def loadschema(*args, **kw):
        logger.info("loading TPC-H schema")
        call(kw, 'load_tpch_schema')
        return 'succ'

    def do_loaddata(*args, **kw):
        dict=copy.copy(_kw_)
        if kw.has_key('parallel'):
          dict["parallel"] = call(kw,"parallel");
          logger.info("load data in parallel %d" % (int(dict['parallel'])))
        else:
          dict["parallel"] = 4
          logger.info("load data in default parallel 4, can change using parallel=x option")
        call(kw, 'load_tpch_data', **dict)
        return 'succ'

    def copydata(*args, **kw):
        dict=copy.copy(_kw_)
        dict["ip"] = call(kw,"ip");
        dict["from"] = call(kw,"from");
        dict["to"] = call(kw,"to");
        if None == dict['from']:
            logger.error("MUST SPECIFY from=xxxx!!!");
            return
        logger.info(dict['from'])
        if None == dict['to']:
            logger.error("MUST SPECIFY to=yyyy!!!");
            return
        logger.info("copy tpch data from %s to %s" % (dict['from'], dict['to']))

        # generate load data file
        call(kw, 'init_load_data_file')
        g = os.walk(dict['from'])
        for path,dir_list,file_list in g:
          for file_name in sorted(file_list):
            if 'tbl' in file_name:
              target = os.path.join(dict['to'], file_name)
              table = file_name.split('.')[0]
              sql_str = "load data /*+ parallel(4) */ infile '%s' into table %s fields terminated by '|';" % (target, table);
              logger.info(sql_str);
              call(kw, 'write_load_data_file', sql_str=sql_str)
        call(kw, 'copy_tpch_data', **dict)
        return 'succ'
    return build_dict(locals(), **__kw)

@ObInstanceBuilder
def ObInstanceLoaddata(**__kw):
    init_load_data_file = '''!sbsh: cat /dev/null > loaddata.sql.tmp'''
    write_load_data_file = '''!sbsh: echo "$sql_str" >> loaddata.sql.tmp'''
    tpch_data = hap_file_path('./loaddata.sql.tmp')
    load_tpch_data = '''!sbsh: cat $tpch_data | xargs -d ";" -I CMD -P $parallel obclient -h $ip -P $mysql_port -utest@oracle -DTEST -ptest -c -e CMD'''
    stream_load_tpch_data = '''!sbsh: cat $tpch_data | xargs -d "\\n" -I CMD -P $parallel bash -c CMD'''

    loadschema = '!call: obs0.loadschema'
    # copy all files and then load data
    do_loaddata = '!call: obs0.do_loaddata'
    copydata = '!call: obs0.copydata'
    loaddata = '''!seq: copydata loadschema do_loaddata'''

    # copy and load by stream
    #streamload = '!call: obs0.streamload'
    #netload = '!call: obs0.netload'
    stream_loaddata = '''!seq: loadschema streamload'''
    net_loaddata = '''!seq: loadschema netload'''

    def streamload(*args, **kw):
        dict=copy.copy(_kw_)
        dict["from"] = call(kw,"from");
        dict["to"] = call(kw,"data_dir");
        if None == dict['from']:
          logger.error("MUST SPECIFY from=xxxx!!!");
          return
        if kw.has_key('parallel'):
          dict["parallel"] = call(kw,"parallel");
          logger.info("load data in parallel %d" % (int(dict['parallel'])))
        else:
          dict["parallel"] = 4
          logger.info("load data in default parallel 4, can change using parallel=x option")
        if kw.has_key('servers'):
          dict["servers"] = int(call(kw,"servers"));
          logger.info("load data via %d servers" % (dict['servers']))
        else:
          dict["servers"] = len(get_match_child(kw, '^.*observer$'))
          logger.info("load data in %d servers, can change using servers=x option" % (dict['servers']))

        # generate load data file
        call(kw, 'init_load_data_file')
        g = os.walk(dict['from'])
        counter = 0
        for path,dir_list,file_list in g:
          for file_name in sorted(file_list):
            if 'tbl' in file_name:
              source = os.path.join(dict['from'], file_name)
              target = os.path.join(dict['to'], file_name)
              table = file_name.split('.')[0]
              ip = call(kw, 'obs{counter}.ip'.format(counter=counter))
              mysql_port = call(kw, 'obs{counter}.mysql_port'.format(counter=counter))
              counter = (counter + 1) % dict['servers']
              sql_str = '''scp {source} {ip}:{target}; obclient -h {ip} -P {mysql_port} -utest@oracle -DTEST -ptest -c -e \\"load data /*+ parallel(4) */ infile '{target}' into table {table} fields terminated by '|'\\"; ssh -t {ip} 'rm {target}';'''.format(source=source, target=target,table=table, ip=ip, mysql_port=mysql_port);
              logger.info(sql_str);
              call(kw, 'write_load_data_file', sql_str=sql_str)
        call(kw, 'stream_load_tpch_data', **dict)
        return 'succ'

    def netload(*args, **kw):
        dict=copy.copy(_kw_)
        if not kw.has_key('scale'):
          dict["scale"] = "tpch_100m"
        else:
          dict["scale"] = call(kw,"scale");
        dict["from"] = "http://11.166.86.153:8877/ob_load_tpch/" + dict['scale'] + "/"
        dict["to"] = call(kw,"data_dir");
        if kw.has_key('parallel'):
          dict["parallel"] = call(kw,"parallel");
          logger.info("load data in parallel %d" % (int(dict['parallel'])))
        else:
          dict["parallel"] = 4
          logger.info("load data in default parallel 4, can change using parallel=x option")
        if kw.has_key('servers'):
          dict["servers"] = int(call(kw,"servers"));
          logger.info("load data via %d servers" % (dict['servers']))
        else:
          dict["servers"] = len(get_match_child(kw, '^.*observer$'))
          logger.info("load data in %d servers, can change using servers=x option" % (dict['servers']))

        # generate load data file
        call(kw, 'init_load_data_file')
        file_list = ['customer.tbl','lineitem.tbl','nation.tbl','orders.tbl','partsupp.tbl','part.tbl','region.tbl','supplier.tbl']
        counter = 0
        for file_name in sorted(file_list):
          if 'tbl' in file_name:
            source = dict['from'] + file_name
            target = os.path.join(dict['to'], file_name)
            table = file_name.split('.')[0]
            ip = call(kw, 'obs{counter}.ip'.format(counter=counter))
            mysql_port = call(kw, 'obs{counter}.mysql_port'.format(counter=counter))
            counter = (counter + 1) % dict['servers']
            sql_str = '''ssh -t {ip} 'wget -nv {source} -O {target}'; obclient -h {ip} -P {mysql_port} -utest@oracle -DTEST -ptest -c -e \\"load data /*+ parallel(4) */ infile '{target}' into table {table} fields terminated by '|'\\"; ssh -t {ip} 'rm {target}';'''.format(source=source, target=target,table=table, ip=ip, mysql_port=mysql_port);
            logger.info(sql_str);
            call(kw, 'write_load_data_file', sql_str=sql_str)
        call(kw, 'stream_load_tpch_data', **dict)
        return 'succ'


    return build_dict(locals(), **__kw)

