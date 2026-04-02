__doc__ += '''
* bquery
./hap.py ob1.bquery [prepare|run|cleanup] [testset=xxxxx]
'''
@Call
def magic_xsh(d, cmd, args, kw):
    cmd_list = shlex.split(cmd)
    real_cmd = list(cmd_list) + list(args)
    if _dryrun_:
        return real_cmd
    return subprocess.call(real_cmd)

@Call
def magic_bqsh(d, cmd, args, kw):
    if (bquery_ip == ''):
        return magic_sh(d, cmd, args, kw)
    else:
        return popen_wrapper(dict_updated(d, _rest_=args), 'ssh -T $usr@$bquery_ip', input=cmd)

sql_user = 'root@sys'
bquery_database='test'
bquery_bin = 'java -cp ./bin/acclog.jar com.taobao.obqa.acclog.cases.AccountDetailCount'
bquery_threads = 64
bquery_ip = ''

@ObServerBuilder
def ObServerMisc(**__kw):

    bquery_updatebin= """!bqsh: cd bquery; wget http://junyue.oss-cn-hangzhou-zmf.aliyuncs.com/obaccount-1.0.0-SNAPSHOT-jar-with-dependencies.jar -O bin/acclog.jar"""
    bquery_deploy  = """!bqsh: git --git-dir=/dev/null clone --depth=1  http://gitlab.alibaba-inc.com/obqa/bquery.git bquery; rm -fr bquery/.git"""
    bquery_run     = """!bqsh: cd bquery; $bquery_bin --username=$sql_user --database=$bquery_database --jdbcurl=jdbc:mysql://$ip:$mysql_port --testsuite=$testsuite --testset=$testset"""
    bquery_testrun = """!bqsh: cd bquery; $bquery_bin --username=$sql_user --database=$bquery_database --jdbcurl=jdbc:mysql://$ip:$mysql_port --testsuite=tests --testset=bquery_basic"""
    bquery_record  = """!bqsh: cd bquery; $bquery_bin --username=$sql_user --database=$bquery_database --jdbcurl=jdbc:mysql://$ip:$mysql_port --testsuite=$testsuite --testset=$testset --record"""
    bquery_prepare = """!bqsh: cd bquery; $bquery_bin --username=$sql_user --database=$bquery_database --jdbcurl=jdbc:mysql://$ip:$mysql_port --testsuite=$testsuite --testset=create_schema,insert"""
    bquery_destroy = """!bqsh: cd bquery; $bquery_bin --username=$sql_user --database=$bquery_database --jdbcurl=jdbc:mysql://$ip:$mysql_port --testsuite=$testsuite --testset=drop_schema --record;"""
    bquery_cleanup = """!bqsh: cd bquery; $bquery_bin $ip $mysql_port --testset=$testset --cleanup"""

    def get_all_tests(testsuite):
      mypath = 'bquery/' + testsuite + '/t/'
      testset = ','.join([os.path.splitext(f)[0] for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f)) and f.endswith('.test') and f != 'create_schema.test' and f != 'drop_schema.test' and not(f.startswith('insert_')) and f != 'insert.test'])
      return testset

    def bquery(*args, **kw):
        #opt = dict()
        #if "testset" in args:
        #    opt["testset"] = args["testset"].split(',')
        result = []

        if 'deploy' in args:
            result.append(['deploy', call(kw, 'bquery_deploy')])
        elif 'updatebin' in args:
            result.append(['updatebin', call(kw, 'bquery_updatebin')])
        elif 'prepare' in args:
            result.append(['prepare', call(kw, 'bquery_prepare')])
        elif 'destroy' in args:
            result.append(['destroy', call(kw, 'bquery_destroy')])
        elif 'record' in args:
            result.append(['record', call(kw, 'bquery_record')])
        elif 'run' in args:
            mysets = _kw_['testset'] if _kw_.has_key('testset') else get_all_tests(_kw_['testsuite'])
            print mysets
            result.append(['run', call(kw, 'bquery_run', testset=mysets)])
        elif 'cleanup' in args:
            result.append(['cleanup', call(kw, 'bquery_cleanup')])
        else:
            result.append(['testrun', call(kw, 'bquery_testrun')])
        return result

    return build_dict(locals(), **__kw)

@ObInstanceBuilder
def ObInstanceMisc(**__kw):
    bquery = '!call: obs0.bquery'
    return build_dict(locals(), **__kw)
