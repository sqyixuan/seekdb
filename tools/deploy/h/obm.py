__doc__ += '''
* obm
./hap.py obxx # gen obm config file
./hap.py obm.[reboot|mysqltest]
'''
from Queue import Queue, Empty

def OBM(**__kw):
    role = "obm"

    # def mysqltest(**kw):
    #     for v in get_match_child(kw, 'obi').values():
    #         call(v, 'mysqltest')

    def __mysqltest_run(testset, **kw):
        stdout = sys.stdout
        # sys.stdout = open(os.devnull, 'w')

        obis = get_match_child(kw, 'obi').values()
        q = Queue()
        rq = Queue()
        concurrency = kw.get('concurrency', len(obis))
        for item in testset:
            q.put(item)
        total_tests = q.qsize()
        proc_tests = 0
        for idx in range(concurrency):
            obi = obis[idx % len(obis)]
            kw = dict_updated(kw, queue=q, result_queue=rq, obi=obi)
            t = Thread(target=__mysqltest_worker, kwargs=kw)
            t.daemon = True
            t.start()
        while total_tests != proc_tests:
            if _error_:
                break;
            while not rq.empty():
                try:
                    result = rq.get(False)[0]
                    proc_tests += 1
                    # mysql_test.pinfo(result['start_ts'], stdout)
                    mysql_test.prun(result['test'], stdout)
                    if result["ret"] == 0:
                        mysql_test.psucc(
                            "%s ( %f s )" % (result['test'], result['cost_time'])
                            , stdout)
                    else:
                        mysql_test.pfail(
                            "%s ( %f s )\n%s" % (result['test'], result['cost_time'],
                                                 mysql_test.shrink_errmsg(result["errput"]))
                            , stdout)
                except Empty:
                    pass
            time.sleep(0.1)

        sys.stdout = stdout

    def __mysqltest_worker(*args, **kw):
        q = kw['queue']
        rq = kw['result_queue']
        obi = kw['obi']
        try:
            item = q.get(False)
            while item:
                try:
                    result = call(obi, "mysqltest", "disable-reboot", testset=item)
                    rq.put(result)
                except Fail:
                    pass
                q.task_done()
                item = q.get(False)
        except Empty, e:
            pass

    def minitest(**kw):
        return __mysqltest_run(mysql_test.mini_test, **kw)

    def smalltest(**kw):
        return __mysqltest_run(mysql_test.small_test, **kw)

    def psmalltest(**kw):
        return __mysqltest_run(mysql_test.psmall_test, **kw)

    def succtest(**kw):
        return __mysqltest_run(mysql_test.succ_test, **kw)

    par_timeout=30
    all_obi = '!filter: is_match role obi'
    id = '!all: all_obi id'
    reboot = '!par[par_timeout=300]: all_obi reboot'
    force_stop = '!par: all_obi force_stop'
    stop = '!par: all_obi stop'
    cleanup = '!par: all_obi cleanup'

    return build_dict(locals(), **__kw)

def obxx(*cluster_norm, **kw):
    concurrency, zones, zservers, proxies = 1, 1, 1, 0
    try:
        if len(cluster_norm) > 0:
            concurrency = int(cluster_norm[0])
        if len(cluster_norm) > 1:
            zones, zservers = cluster_norm[1].split('x')
            if zservers is None:
                zservers = 1
        if len(cluster_norm) > 2:
            proxies = int(cluster_norm[2])
    except ValueError as e:
        info("USAGE: obxx CONCURRENCY NORM PROXIES")
        info("EXAMPLE: obxx 3 2x2 1")
        info("use default: [1x1 0] instead")

    url_tmp = "http://new.oceanbase.info:8080/conf/deploy?concurrency=%d&zones=%d&zservers=%d&zproxys=%d"
    url = url_tmp % (concurrency, int(zones), int(zservers), int(proxies))

    try:
        result = urllib2.urlopen(url).read()
    except Exception as e:
        info("please conntact fufeng.syd: %s %s"%(url, e))
        sys.exit(1)

    print
    print result
    with open('./config.obx.py', 'w') as obx_fp:
        obx_fp.write(result)

    sys.exit(0)

if os.path.isfile('config.obx.py'):
    load_file('config.obx.py')
