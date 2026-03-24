__doc__ += '''
* mysqltest
./hap.py ob1.mysqltest [_quiet_=True] ['testset=test1[,test2[,...]]'] ['testpat=rowkey_*'] [record|test]  # start mysqltest, more info plz dig into mysql_test dir
./hap.py ob1.proxy0.mysqltest # run mysqltest on proxy0
'''
import os
import mysql_test
from regress_suite_map import *
import hashlib
import datetime
try:
   from subprocess import Popen, PIPE, TimeoutExpired
except ImportError:
   from subprocess32 import Popen, PIPE, TimeoutExpired
check_ob_version()

last_stack_time = datetime.datetime.min
def pid_stack(call):
  global last_stack_time
  now = datetime.datetime.now()
  if now - last_stack_time > datetime.timedelta(minutes=1):
    call()
    last_stack_time = now

def force_info(msg):
    logger.error(msg)

localdir = '$local_dir'
@ObServerBuilder
def ObServerMysqltest(**__kw):
    def minitest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        mini=('mini',)
        newargs=args + mini
        return call(ob, 'mysqltest',*newargs,**dict)
    def schematest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        schema=('schema',)
        newargs=args + schema
        logger.error(newargs)
        return call(ob, 'mysqltest',*newargs,**dict)
    def ddltest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        ddl=('ddl',)
        newargs=args + ddl
        logger.error(newargs)
        return call(ob, 'mysqltest',*newargs,**dict)
    def pxtest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        px=('px',)
        newargs=args + px
        logger.error(newargs)
        return call(ob, 'mysqltest',*newargs,**dict)
    def smalltest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        small=('small',)
        newargs=args + small
        return call(ob, 'mysqltest',*newargs,**dict)
    def psmalltest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        psmall=('psmall',)
        newargs=args + psmall
        return call(ob, 'mysqltest',*newargs,**dict)
    def succtest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        succ=('succ',)
        newargs=args + succ
        return call(ob, 'mysqltest',*newargs,**dict)
    def mysqltest(*args, **attr):
        ob=attr.get('__parent__')
        dict=copy.copy(_kw_)
        dict["ip"] = call(attr,"ip");
        dict["port"] = call(attr,"mysql_port");
        dict["role"] = call(attr,"role");
        dict["observerdir"] = call(attr,"observerdir");
        return call(ob, 'mysqltest',*args,**dict)
    return build_dict(locals(), **__kw)

@ObInstanceBuilder
def ObInstanceMySqltest(**__kw):
    collect = True
    def check_ob_version(**ob):
        if os.getenv("COMMIT") and os.getenv("BRANCH"):
            revision = "1-%s" % os.getenv("COMMIT")
        else:
            revision = popen(r'bin/observer  --version 2>&1  |grep REVISION | sed "s#REVISION: ##g"').strip()
        if not revision and 'observerdir' in ob:
            revision = popen(r'%s/bin/observer  --version 2>&1  |grep REVISION | sed "s#REVISION: ##g"' % ob["observerdir"]).strip()
        if not revision and 'observerdir' in ob:
            revision = popen(r'%s/../bin/observer  --version 2>&1  |grep REVISION | sed "s#REVISION: ##g"' % ob["observerdir"]).strip()
        if not revision and 'observerdir' in ob:
            revision = popen(r'%s/../*.obs0/bin/observer  --version 2>&1  |grep REVISION | sed "s#REVISION: ##g"' % ob["observerdir"]).strip()
        if not revision:
            revision = popen(r'bin/rootserver  --version 2>&1  |grep BUILD_VERSION | sed "s#BUILD_VERSION: ##g"').strip()
        return revision
    def check_ob_branch(**ob):
        if os.getenv("COMMIT") and os.getenv("BRANCH"):
            branch = os.getenv("BRANCH").split("origin/")[-1]
        else:
            version = popen(r"bin/observer  --version 2>&1  |grep '(OceanBase ' | awk -F 'OceanBase' '{print $2}'").strip()
            if not version and 'observerdir' in ob:
                version = popen(r"%s/bin/observer  --version 2>&1  |grep '(OceanBase ' | awk -F 'OceanBase' '{print $2}'" % ob["observerdir"]).strip()
            if not version and 'observerdir' in ob:
                version = popen(r"%s/../bin/observer  --version 2>&1  |grep '(OceanBase ' | awk -F 'OceanBase' '{print $2}'" % ob["observerdir"]).strip()
            if not version and 'observerdir' in ob:
                version = popen(r"%s/../*.obs0/bin/observer  --version 2>&1  |grep '(OceanBase ' | awk -F 'OceanBase' '{print $2}'" % ob["observerdir"]).strip()
            if not version:
                version = popen(r"bin/rootserver  --version 2>&1  |grep '(OceanBase ' | awk -F 'OceanBase' '{print $2}'").strip()
            version = version[0:5]
            branch = ''
            if ('1.' in version or '2.' in version):
                branch = popen(r'git status | grep "^# On branch " | sed "s#\# On branch ##g"').strip()
                if not branch:
                    branch = popen(r"git symbolic-ref --short HEAD").strip()
            elif version == '0.5.2':
                branch = '0_5_2_dev'
            else:
                branch = 'UNKNOWN'
        return branch
    def create_tenant(*args, **ob):
        if "tenant" not in ob or ob["tenant"] == "sys":
            return 0
        sql = '''"create resource unit box_%s max_cpu 1, memory_size '1G';"'''%ob["tenant"]
        cmd = 'echo %s | $mysql_cmd -h ${obs0.ip} -P ${obs0.mysql_port} -uadmin -padmin -Doceanbase'%(sql)
        if sh(sub(cmd, ob)):
            return 1
        sql = '''"create resource pool pool_%s  unit = 'box_%s', unit_num = 32, zone = '${obs0.zone}'"'''%(ob["tenant"],ob["tenant"])
        cmd = 'echo %s | $mysql_cmd -h ${obs0.ip} -P ${obs0.mysql_port} -uadmin -padmin -Doceanbase'%(sql)
        if sh(sub(cmd, ob)):
            return 1
        sql = '''"create tenant %s replica_num = 1, primary_zone='${obs0.zone}' set ob_tcp_invited_nodes='%', resource_pool_list=('pool_%s')"'''%(ob["tenant"],ob["tenant"])
        cmd = 'echo %s | $mysql_cmd -h ${obs0.ip} -P ${obs0.mysql_port} -uadmin -padmin -Doceanbase'%(sql)
        if sh(sub(cmd, ob)):
            return 1
        cmd = 'echo "create database test" | $mysql_cmd -h ${obs0.ip} -P ${obs0.mysql_port} -uroot@%s'%(ob["tenant"])
        return sh(sub(cmd, ob))

    def do_purge(*args, **ob):
        prefix = 'echo "purge recyclebin" | $mysql_cmd -h ${obs0.ip} -P ${obs0.mysql_port}'
        sh(sub(prefix + ' -uroot@mysql', ob))
        if IS_BUSINESS:
            sh(sub(prefix + ' -usys@oracle', ob))

    def quicktest(*args, **ob):
        logger.disable_log()
        mysql_test.pinfo('run libmysql quicktest ...')
        call(ob, "mysqltest","quick","disable-reboot", "merge");
        mysql_test.pinfo('run libmysql+ps quicktest ...')
        call(ob, "mysqltest","quick","disable-reboot","ps");
        mysql_test.pinfo('run java quicktest ...')
        call(ob, "mysqltest","quick","disable-reboot","java");
        mysql_test.pinfo('run java+ps quicktest ...')
        call(ob, "mysqltest","quick","disable-reboot","java","ps");


    def minitest(*args, **ob):
        logger.disable_log()
        mysql_test.pinfo('run libmysql minitest ...')
        mini=('mini',)
        newargs=args + mini
        call(ob, "mysqltest",*newargs,**ob);

    def schematest(*args, **ob):
        logger.disable_log()
        mysql_test.pinfo('run libmysql schematest ...')
        schema=('schema',)
        newargs=args + schema
        call(ob, "mysqltest",*newargs,**ob);

    def ddltest(*args, **ob):
        logger.disable_log()
        mysql_test.pinfo('run libmysql ddltest ...')
        ddl=('ddl',)
        newargs=args + ddl
        call(ob, "mysqltest",*newargs,**ob);

    def pxtest(*args, **ob):
        logger.disable_log()
        mysql_test.pinfo('run libmysql pxtest ...')
        px=('px',)
        newargs=args + px
        call(ob, "mysqltest",*newargs,**ob);

    def smalltest(*args, **ob):
        gcfg['_quiet_'] = True
        mysql_test.pinfo('run libmysql smalltest ...')
        small=('small',)
        newargs=args + small
        call(ob, "mysqltest",*newargs,**ob);

    def psmalltest(*args, **ob):
        logger.disable_log()
        mysql_test.pinfo('run libmysql smalltest ...')
        psmall=('psmall',)
        newargs=args + psmall
        call(ob, "mysqltest",*newargs,**ob);

    def succtest(*args, **ob):
        logger.disable_log()
        mysql_test.pinfo('run libmysql succtest ...')
        succ=('succ',)
        newargs=args + succ
        call(ob, "mysqltest",*newargs,**ob);
    # process suite to tag mapping file, where we read the regress_suite_map file
    # which is a pre-defined test suite to tags mapping file. After that,
    # remove the regress_suite parameter from kw and replace with tags parameter with
    # tags list. See regress_suite_map file to understand the mapping
    # relationship. Note that we only support two level mapping, who wants to use
    # more than that...
    def process_suite2tags(kw) :
        global suite2tags
        global composite_suite

        suites = kw['regress_suite'].split(',')
        tag_list = ''

        for suitename in suites :
            # process composite suite
            if suitename in composite_suite.keys() :
                suites = composite_suite[suitename]
                suite_list = suites.split(',')
                tags = ''
                for name in suite_list :
                    if name in suite2tags.keys() :
                        if suite2tags[name] :
                            tags += suite2tags[name] + ','
                    else :
                        tags += name + ','
                tag_list += tags

            if suitename in suite2tags.keys() :
                if suite2tags[suitename] :
                    tag_list += suite2tags[suitename] + ','
        tag_list = tag_list[0:len(tag_list)-1]

        kw['tags'] = tag_list
        del kw['regress_suite']


    def mysqltest(*args, **ob):
        if 'regress_suite' in ob.keys() :
            process_suite2tags(ob)
            logger.info('running sqltest w/ tags=%s'%(ob['tags']))
        def do_before_test(mgr):
            if need_reboot and mgr.reboot:
#                reboot_again = True
                max_reboot_times=5
                reboot_times=0
                while reboot_times < max_reboot_times:
                    reboot_times += 1
                    mysql_test.pinfo("rebooting...")
                    try:
                        ret = call(ob, 'reboot')[-1][-1]
                    except Exception as e:
                        force_info('reboot exception, %s'%(e))
                        ret = -1
                    if  ret != 0:
                        mysql_test.pinfo("reboot failed, retry after 30 seconds, auto retry = (%d/%d)" % (reboot_times, max_reboot_times))
                        force_info("reboot failed, retry after 30 seconds")
                        time.sleep(30)
                    else:
                        reboot_times = max_reboot_times + 1
                        if call(ob,'create_tenant'):
                            force_info("create tenant failed")
                if reboot_times == max_reboot_times:
                    mysql_test.pinfo("reboot failed %d times, force exit" % reboot_times)
                    sys.exit(-1)
            if need_check_memory:
                try:
                    sql = '''"call oceanbase.check_prepare();"'''
                    cmd = 'echo %s | $mysql_cmd -h ${obs0.ip} -P ${obs0.mysql_port} -uadmin -padmin -Doceanbase'%(sql)
                    sh(sub(cmd, ob))
                except Exception as e:
                    pass

        def do_after_test(mgr, result):
            if result['ret'] != 0:
                if need_collect:
                    # skip pstack
                    #try:
                    #    call(ob, 'pid_stack', run_id=mgr.test)
                    #    time.sleep(10)
                    #    call(ob, 'pid_stack', run_id=mgr.test)
                    #except Exception as e:
                    #    force_info("observer is not exist,please check if there is core or other exception")
                    if need_collect_all:
                        call(ob, 'collect_log_all', run_id=mgr.test)
                    else:
                        call(ob, 'collect_log', run_id=mgr.test)
                if _error_:
                    mgr.stop = True;
                test_if_server_alive(result)
            else:
              try:
                pid_stack(lambda : call(ob, 'obs0.pid_ob_stack', run_id=mgr.test))
                call(ob, 'do_purge')
              except Exception as e:
                pass
            if need_check_memory:
                try:
                    sql = '''"call oceanbase.check_memory();"'''
                    cmd = 'echo %s | $mysql_cmd -h ${obs0.ip} -P ${obs0.mysql_port} -uadmin -padmin -Doceanbase'%(sql)
                    sh(sub(cmd, ob))
                except Exception as e:
                    pass


        def do_after_test_local(mgr, result):
            if result['ret'] != 0:
                if need_collect:
                    #try:
                    #    call(ob, 'pid_stack_local', run_id=mgr.test)
                    #    time.sleep(10)
                    #    call(ob, 'pid_stack_local', run_id=mgr.test)
                    #except Exception as e:
                    #    time.sleep(100)
                    #    force_info("observer is not exist,please check if there is core or other exception")
                    if need_collect_all:
                        call(ob, 'collect_log_all_local', run_id=mgr.test)
                    else:
                        call(ob, 'collect_log_local', run_id=mgr.test)

                if _error_:
                    mgr.stop = True;
            else:
              try:
                pid_stack(lambda : call(ob, 'obs0.pid_ob_stack_local', run_id=mgr.test))
                call(ob, 'do_purge')
              except Exception as e:
                pass

        def test_if_server_alive(result):
            all_server_alive = True
            tmp_ret = call(ob, 'pid')
            for svr_pair in tmp_ret:
               if len(svr_pair[1]) == 0:
                  all_server_alive = False
                  break
            if not all_server_alive:
               result["major_fail"] = 1

        if '_log_' not in ob:
            logger.disable_log()
        need_reboot,need_collect,use_mpi,need_collect_all,need_check_memory = True,True,False,False,False
        opt = dict(mysql_test.opt)
        opt["port"] = call(ob, 'obs0.mysql_port')
        opt["host"] = call(ob, 'obs0.ip')
        opt["observerdir"] = call(ob, 'obs0.observerdir')
        if "ip" in ob:
            opt["host"] = ob["ip"]
            opt["port"] = ob["port"]
            opt["observerdir"] = ob["observerdir"]

        opt["filter"] = 'c'
        opt["role"] = ob["role"]

        if "profile" in args:
            opt["profile"] = True
            opt["record"] = True
        if "java" in args:
            opt["java"] = True
            opt["filter"] = 'j'
            opt["full_user"] = opt["user"] + '@sys#' + call(ob, 'obs0.app_name')
            opt["rslist_url"] = call(ob, 'obs0.cfg_url')
        if "ps" in args:
            opt["filter"] = opt["filter"] + 'p'

        if ob.has_key('keyword'):
            opt["keyword"]=ob['keyword']

        if 'nogroup' in args:
            opt["nogroup"]=True

        method_mode = _cmd_.split('.')[1]

        if "obs" in method_mode and method_mode != 'obs0':
            opt["filter"]='slave'

        if "proxy" in method_mode:
            opt["filter"]='proxy'

        if "quick" in args or "quicktest" in args:
            opt["test-set"] = ['a_trade_quick','hints','create','count_distinct','join_basic','group_by_1','sq_from','compare', 'ps_complex', 'update','func_in', 'row','insert_rows_sum_of_2M_size']
            if "merge" in args:
              opt["test-set"] = ['merge_basic','dump'] + opt["test-set"]
            need_reboot = False
            need_collect = False
        if "modest" in args:
            opt["test-set"] = ['func_equal', 'func_group_1', 'func_group_2', 'func_group_3', 'func_group_4','func_group_5', 'func_group_6', 'func_group_7', 'func_like', 'func_in_2', 'build_in_func_test']
            need_collect = False
        if "mini" in args:
            opt["test-set"] = mysql_test.mini_test
            need_reboot = False
            need_collect = True
        if "schema" in args:
            opt["test-set"] = mysql_test.schema_test
            need_reboot = False
            need_collect = True
        if "ddl" in args:
            opt["test-set"] = mysql_test.ddl_test
            need_reboot = False
            need_collect = True
        if "px" in args:
            opt["test-set"] = mysql_test.px_test
            opt["use-px"] = True
            need_reboot = False
            need_collect = True
        if "small" in args:
            opt["test-set"] = mysql_test.small_test
            need_collect = True
        if "psmall" in args:
            opt["test-set"] = mysql_test.psmall_test
            opt["source-limit"] = mysql_test.psmall_source
            need_collect = True
        if "succ" in args:
            if "testset" in ob:
                opt["test-set"] = ob["testset"].split(',')
            else:
                opt["all"] = 'all'
                opt["suite"] = ','.join(os.listdir(os.path.realpath("mysql_test/test_suite")))
                opt["succ"] = "succ"
            need_collect = True
        if "record" in args:
            opt["record"] = True
        if "use-px" in args:
            opt["use-px"] = True
        if "force-explain-as-px" in args:
            opt["force-explain-as-px"] = True
        if "force-explain-as-no-px" in args:
            opt["force-explain-as-no-px"] = True
        if "force-explain-as-px" in args and "force-explain-as-no-px" in args:
          force_info("can not use force-explain-as-px and force-explain-as-no-px the same time")
          sys.exit(1)
        if "oblog_diff" in args:
            opt["oblog_diff"] = True
        if "java" in args:
            if 'jar' in ob and ob['jar'] == 'local':
                get_jar_cmd="cp ../obtest/jar/mytest.jar ./mysql_test/java/mytest.jar"
            else:
                get_jar_cmd="wget http://11.166.86.153:8877/mytest10.jar -o ./mytest.down -O ./mysql_test/java/mytest.jar"
            #get_jar_cmd="wget http://11.166.86.153:8877/mytest_for_oracle.jar -o ./mytest.down -O ./mysql_test/java/mytest.jar"
            ver_req = '1.8'
            java_ver="version=$(java -version 2>&1 | awk -F '\"' '/version/ {print $2}');"
            java_ver_test= java_ver + "[ \"$version\" \\> '{0}' ]; ".format(ver_req)
            java_ver_ret=sh(java_ver_test)
            if java_ver_ret != 0:
                raise EnvironmentError(" version is less than {0}".format(ver_req))
            target='./mysql_test/java/mytest.jar'

            link='http://11.166.86.153:8877/mytest_for_oracle.md5'

            f = urllib.urlopen(link)
            checksum = f.read()

            if os.path.exists(target) == False or hashlib.md5(open(target, 'rb').read()).hexdigest() != checksum:
                force_info("execute cmd: " + get_jar_cmd)
                sh(get_jar_cmd)
            if  os.path.exists(target) == False:
                force_info("failed to execute cmd: " + get_jar_cmd)
                sys.exit(1)
            addr = os.getenv('PWD')+"/mysql_test/java:" + os.getenv('PATH')
            os.environ['PATH'] = addr
        else:
            addr = os.getenv('PWD')+":" + os.getenv('PATH')
            os.environ['PATH'] = addr

        if "ps" in args:
            opt['ps_protocol'] = True
        elif "test" in args:
            opt["record"] = False
        if "disable-reboot" in args:
            need_reboot = False
        if "check-memory" in args:
            need_check_memory = True
        opt["engine"] = "None"
        if "engine" in ob.keys():
            if ob["engine"] in ("old", "new"):
                opt["engine"] = ob["engine"]
            else:
                opt["engine"] = "old" # TODO change to 'new'
        if "use-mpi" in args:
            use_mpi = True
        if "tenant" in ob:
            opt["tenant"] = ob["tenant"]
        if "collect" in ob:
            if ob["collect"] == 'False':
                need_collect = False
        if "mode" in ob:
             opt["mode"] = ob["mode"]
        else:
             opt["mode"] = 'both'
        if "testset" in ob:
            opt["test-set"] = ob["testset"].split(',')
            #like ./deploy.py ob1.mysqltest testset=NOT GIVEN, just return
            if len(opt["test-set"]) == 1 and opt["test-set"][0] == "":
                return True

        if "testpat" in ob:
            # remove "test-set" that mysqltest'll ignore it
            if "test-set" in opt:
                del opt["test-set"]
            opt["test-pattern"] = ob["testpat"]
        if "group" in ob:
            opt["owner-group"] = [group.strip() for group in ob["group"].split(',')]
        if "owner" in ob:
            opt["owner"] = [owner.strip() for owner in ob["owner"].split(',')]
        if "tags" in ob:
            opt["test-tags"] = [tag.strip() for tag in ob["tags"].split(',')]

        if "obfarm" in ob:
            opt["obfarm"] = ob["obfarm"].strip()
        if "only" in args:
            need_collect = False
            opt["only"] = True
        if "oracle" in ob:
            logger.debug("****oracle****");
            try:
                conn_str = ob["oracle"]
                prof_str = conn_str.split('@')[0]
                host_str = conn_str.split('@')[1]
                opt["oracle_user"] = prof_str.split('/')[0]
                opt["oracle_password"] = prof_str.split('/')[1]
                opt["oracle_host"] = host_str.split(':')[0]
                opt["oracle_port"] = host_str.split(':')[1].split('/')[0]
                opt["oracle_database"] = host_str.split(':')[1].split('/')[1]
                if "only" in args:
                    need_reboot = False
                    need_collect = False
            except:
                raise Exception("str format is WRONG, should be 'user/passwd@ip:port/sid', but get '%s'" % conn_str)
        if "mysql" in ob:
            logger.debug("****mysql****");
            opt["my_port"] = ob["mysql"].strip().split(':')[1]
            opt["my_host"] = ob["mysql"].strip().split(':')[0]
            opt["my_user"] = "root"
            opt["my_password"] = ""
            if "only" in args:
                need_reboot = False
                need_collect = False
        #opt['user'] = find(ob, 'sql_user')
        if "suite" in ob:
            opt["suite"] = ob["suite"].strip()

        if "collect_all" in args:
            need_collect_all= True

        if "all" in args:
            opt["all"] = 'all'
            opt["suite"] = ','.join(os.listdir(os.path.realpath("mysql_test/test_suite")))

        if 'slices' in ob and 'slice_idx' in ob:
            opt['slices'] = ob['slices']
            opt['slice_idx'] = ob['slice_idx']
        if 'slb' in ob:
            opt['slb_id']=ob["slb"].split(',')[1]
            opt['slb_host']=ob["slb"].split(',')[0]
        if 'auto-retry' in args:
            opt['auto-retry'] = True
        else:
            opt['auto-retry'] = False
        opt["OB_VERSION"] = call(ob,'check_ob_version')
        opt["OB_BRANCH"]  = call(ob,'check_ob_branch')

        mgr = mysql_test.Manager(opt)

        mgr.before_one = do_before_test
        if 'is_local' in ob and ob['is_local']:
            mgr.after_one = do_after_test_local
        else:
            mgr.after_one = do_after_test
        os.putenv('OBMYSQL_PORT', str(mgr.opt["port"]))
        os.putenv('OBMYSQL_MS0', str(mgr.opt["host"]))
        os.putenv('OBMYSQL_PWD', str(mgr.opt["password"]))
        os.putenv('LOCAL_DIR', str(call(ob, 'localdir')))
        os.putenv('OBSERVER_DIR', str(mgr.opt["observerdir"]))
        global IS_BUSINESS

        if IS_BUSINESS:
            os.environ['IS_BUSINESS']='1'
            os.putenv('IS_BUSINESS', str(1))
        else:
            os.environ['IS_BUSINESS']='0'
            os.putenv('IS_BUSINESS', str(0))
        return mgr.start()
    return build_dict(locals(), **__kw)
