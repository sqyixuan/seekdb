#!/usr/bin/env python2.6
# ecoding=utf-8
# This file defines some structs and its operations about mysql test,
# and will be part of `deploy.py' script.
# Author: Fufeng
# Last change: 2013-04-26 14:17:05 #

import re
import os 
import operator
IS_BUSINESS=True

def check_ob_version():
    cmd=os.popen('./bin/observer   --version 2>&1|grep -i \'OceanBase_CE 4\'')
    res=cmd.read()
    i = "OceanBase_CE 4" in res
    global IS_BUSINESS
    if operator.contains(res,"OceanBase_CE 4"):
       
        IS_BUSINESS=False
        print('mysqltest check the ob version is CE')
    else:
        
        IS_BUSINESS = True
        print('mysqltest check the ob version is business')
    
        

check_ob_version()
def get_exclude_case_by_base_name(base_name):
    global IS_BUSINESS
    global case_list
    #If it is the commercial version
    if IS_BUSINESS:
        return case_list[base_name+"_list"]
    else:
        # Does the corresponding community version caselist exist?
        if base_name+"_ce_list" in case_list.keys():
            return case_list[base_name+"_ce_list"]
        else:
            return case_list[base_name+"_list"]
try:
    from subprocess import Popen, PIPE, TimeoutExpired 
except ImportError:
    from subprocess32 import Popen, PIPE, TimeoutExpired 
import shlex
import glob
import errno
import pprint
import socket
from os import makedirs,rename,popen,listdir
from os.path import basename, join, dirname, realpath, abspath, exists
from time import strftime, time, sleep
import sys
from filter import *
from succtest import *
from rebootcase import *
import json
import urllib2
import urllib
import httplib
from test_tags import *
import os
from testcg import *

mtdir = dirname(realpath(__file__))

def read(path):
    f = open(path, 'r')
    content = f.read()
    f.close()
    return content


def my_mkdir(path):
    try:
        makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise

def shrink_errmsg(errmsg):
    if isinstance(errmsg, str):
        errmsg = errmsg.decode("utf-8", "replace")
    if isinstance(errmsg, unicode):
        return re.split("\nThe result from queries just before the failure was:", errmsg, 1)[0]
    elif isinstance(errmsg, Exception):
        return errmsg
    else:
        return "UNKNOWN ERR MSG"


def pheader(msg, fp=None):
    if fp is None:
        fp = sys.stdout
    fp.write('\033[32m[==========]\033[0m %s\n' % msg.encode("utf-8"))
    fp.flush()

def pinfo(msg, fp=None):
    if fp is None:
        fp = sys.stdout
    fp.write('\033[32m[----------]\033[0m %s\n' % msg.encode("utf-8"))
    fp.flush()

def prun(msg, fp=None):
    if fp is None:
        fp = sys.stdout
    fp.write('\033[32m[ RUN      ]\033[0m %s\n' % msg.encode("utf-8"))
    fp.flush()

def pslb(msg, fp=None):
    if fp is None:
        fp = sys.stdout
    fp.write('\033[33m[ SLB INFO ]\033[0m %s\n' % msg.encode("utf-8"))
    fp.flush()

def pwarn(msg, fp=None):
    if fp is None:
        fp = sys.stdout
    fp.write('\033[33m[  WARN    ]\033[0m %s\n' % msg.encode("utf-8"))
    fp.flush()

def pfail(msg, fp=None):
    if fp is None:
        fp = sys.stdout
    fp.write('\033[31m[  FAILED  ]\033[0m %s\n' % msg.encode("utf-8"))
    fp.flush()

def psucc(msg, fp=None):
    if fp is None:
        fp = sys.stdout
    fp.write('\033[32m[       OK ]\033[0m %s\n' % msg.encode("utf-8"))
    fp.flush()

def ppasslst(msg):
    print '\033[32m[ PASS LST ]\033[0m %s' % msg.encode("utf-8")
    sys.stdout.flush()

def ppass(msg):
    print '\033[32m[  PASSED  ]\033[0m %s' % msg.encode("utf-8")
    sys.stdout.flush()

def pfaillst(msg):
    print '\033[31m[ FAIL LST ]\033[0m %s' % msg.encode("utf-8")
    sys.stdout.flush()

def find_tag_test_with_file_pat(file_pattern, flag_pattern, tag, filelist):
    for test in glob.glob(file_pattern):
        if "test_suite/" in test:
            if dirname(test).split('/')[-2] == tag:
                filelist.append(test)
                continue
        test_file = open(test, 'r')
        line_num = 0
        for line in test_file:
            line_num += 1
            if line_num > 30:
                break
            matchobj = re.search(flag_pattern, line)
            if matchobj:
                tag_set = line.split(':')[1].split(',')
                for tag_tmp in tag_set:
                    tag_t = tag_tmp.strip()
                    if tag.lower() == tag_t.lower():
                        filelist.append(test)

def find_tag_tests(flag_pattern, tags):
    filelist = []
    for tag in tags:
        test_pattern = join(mtdir, "t/*.test")
        find_tag_test_with_file_pat(test_pattern, flag_pattern, tag, filelist)
        test_pattern = join(mtdir, "test_suite/*/t/*.test")
        find_tag_test_with_file_pat(test_pattern, flag_pattern, tag, filelist)
    return filelist

def check_tag_exists(test_file_path, flag_pattern, tag):
  test_file = open(test_file_path, 'r')
  line_num = 0
  for line in test_file:
      line_num += 1
      if line_num > 30:
          break
      matchobj = re.search(flag_pattern, line)
      if matchobj:
          tag_set = line.split(':')[1].split(',')
          for tag_tmp in tag_set:
              tag_t = tag_tmp.strip()
              if tag.lower() == tag_t.lower():
                  return True
  return False

def test_name(test_file):
    if "test_suite/" in test_file:
        suite_name = dirname(test_file).split('/')[-2]
        base_name = basename(test_file).rsplit('.')[0]
        return suite_name + '.' + base_name
    else:
        base_name = basename(test_file).rsplit('.')[0]
        return base_name


class Arguments:
    def add(self, k, v = None):
        self.args.update({k:v});

    def __str__(self):
        str = ""
        for k,v in self.args.items():
            if v != None:
                if re.match("^--\w", k):
                    str += " %s=%s" % (k, v)
                else:
                    str += " %s %s" % (k, v)
            else:
                str += " %s" % k
        return str
    def __init__(self, opt):
        self.args = dict()
        try:
            if "connector" in opt and "java" in opt:
                self.add("--connector", opt["connector"]);
            self.add("--host", opt["host"])
            self.add("--port", opt["port"])
            self.add("--tmpdir", opt["tmp-dir"])
            self.add("--logdir", "%s/log" % opt["var-dir"])
            my_mkdir(opt["tmp-dir"])
            my_mkdir("%s/log" % opt["var-dir"])
        except Exception as e:
            print "[Error] %s option not specify in configure file" % e
            raise
        self.add("--silent")
        # our mysqltest doesn't support this option
        # self.add("--skip-safemalloc")
        self.add("--user", 'root')
        if "user" in opt:
            user = opt["user"]
            if "connector" not in opt or opt["connector"] == "ob":
                user = user + '@' + opt['case_mode'];
            self.add("--user", user)
        if "password" in opt:
            self.add("--password", opt["password"])
        if "full_user" in opt:
            self.add("--full_username", opt["full_user"].replace('sys',opt['case_mode']))
        if "tenant" in opt:
            self.add("--user", 'root@' + opt["tenant"])
            self.add("--password", '')
            self.add("--full_username", 'root@' + opt["tenant"] + '#' + call(ob, 'obs0.app_name'))
        if "rslist_url" in opt:
            self.add("--rslist_url", opt["rslist_url"])
        if "database" in opt:
            self.add("--database", opt["database"])
        if "charsetdsdir" in opt:
            self.add("--character-sets-dir", opt["charsetsdir"])
        if "basedir" in opt:
            self.add("--basedir", opt["basedir"])
        if "use-px" in opt:
            self.add("--use-px")
        if "force-explain-as-px" in opt:
            self.add("--force-explain-as-px")
        if "force-explain-as-no-px" in opt:
            self.add("--force-explain-as-no-px")
        if "mark-progress" in opt:
            self.add("--mark-progress")
        if "ps_protocol" in opt:
            self.add("--ps-protocol")
        if "sp_protocol" in opt:
            self.add("--sp-protocol")
        if "view_protocol" in opt:
            self.add("--view-protocol")
        if "cursor_protocol" in opt:
            self.add("--cursor-protocol")
        # if "strace_client" in opt:
        #     exe = opt["strace_client"] || "strace";
        #     self.add("-o", "%s/log/mysqltest.strace %s_mysqltest" % (opt["vardir"], exe)

        self.add("--timer-file", "%s/log/timer" % opt["var-dir"]);

        if "compress" in opt:
            self.add("--compress")
        if "sleep" in opt:
            self.add("--sleep", "%d" % opt["sleep"])
        # # Turn on SSL for _all_ test cases if option --ssl was used
        # if "ssl" in opt:
        #     self.add("--ssl")
        if "max_connections" in opt:
            self.add("--max-connections", "%d" % opt["max_connections"])

        if "test-file" in opt:
            self.add("--test-file", opt["test-file"])

        # Number of lines of resut to include in failure report
        self.add("--tail-lines", ("tail-lines" in opt and opt["tail-lines"]) or 20);
        if "oblog_diff" in opt and opt["oblog_diff"]:
            self.add("--oblog_diff")

        if "record" in opt and opt["record"] and "record-file" in opt:
            self.add("--record")
            self.add("--result-file", opt["record-file"])
        else:                                    # diff result & file
            self.add("--result-file", opt["result-file"])

class Tester:
    def __init__(self):
        pass

    def find_bin(self, opt):
        if 'mysqltest-bin' in opt:
            # check version
            #version = Popen([opt['mysqltest-bin'], "--version"], stdout=PIPE)
            #if version.communicate()[0].find("Ver 3.3 Distrib 5.5.27") != -1:
            return opt["mysqltest-bin"]
            #else:
            #    return None
        else:
            try:
                p = Popen(["which", "mysqltest"], stdout=PIPE)
                path = p.communicate()[0]
                if p.returncode == 0:
                    # check version
                    #version = Popen([path.strip(), "--version"], stdout=PIPE)
                    #if version.communicate()[0].find("Ver 3.3 Distrib 5.5.27") != -1:
                    return path
                    #else:
                    #    return None
                else:
                    print("Please set `mysqltest-bin' option")
                    return None
            except Exception as e:
                print("%s, Please set `mysqltest-bin' option" % e)
                return None

    def call_sql_scripts(self, script, tout, opt):
        if os.path.exists(script):
          call_sql_scripts_cmd = "obclient -h " + str(opt["host"]) + " -P" + str(opt["port"]) + " -uroot -Doceanbase -c -e  \"source " + script + "\"";
          #print "execute", call_sql_scripts_cmd
          os.popen(call_sql_scripts_cmd)

    def run(self, test, tout, opt):
        if None == self.find_bin(opt):
            exit(-1)

        opt["test-file"] = join(opt["test-dir"], test + ".test")
        if opt['mode'] == 'mysql' or opt['mode'] == 'oracle':
           opt['case_mode'] = opt['mode']
        if opt['mode'] == "both":
           if test.endswith("_mysql"):
               opt['case_mode'] = 'mysql'
           if test.endswith("_oracle"):
               opt['case_mode'] = 'oracle'

        # support explain select w/o px hit
        # the result file directory of force-explain-xxxx is
        # - explain_r/mysql
        # - explain_r/oracle
        # The directory for the remaining result files is
        # - r/mysql
        # - r/oracle
        suffix = ""
        opt_explain_dir = ""
        if "force-explain-as-px" in opt:
          suffix = ".use_px"
          opt_explain_dir = "explain_r/"
        elif "force-explain-as-no-px" in opt:
          suffix = ".no_use_px"
          opt_explain_dir = "explain_r/"

        opt["result-dir"] = "".join([opt["result-dir"], "/", opt_explain_dir, opt['case_mode'], "/"])
        if "slave" == opt["filter"]:
            opt["slave-cmp"] = 1;
            if os.path.exists("".join([opt["result-dir"], "/", test + suffix + ".slave.result"])):
                opt["slave-cmp"] = 0
                suffix += ".slave"

        if "old" == opt["engine"]:
            suffix += ".old"

        if suffix.endswith(".old") and not os.path.exists("".join([opt["result-dir"], "/", test + suffix + ".result"])):
           suffix = suffix[:-4]
        #opt["result-file"] = "".join([opt["result-dir"], "/", test + suffix + ".result"])
        if IS_BUSINESS:
            opt["result-file"] = "".join([opt["result-dir"], "/", test + suffix + ".result"])
        else:
            result_file= "".join([opt["result-dir"], test + suffix +".ce"+ ".result"])
            if os.path.exists(result_file):
                opt["result-file"] = result_file
            else:
                opt["result-file"] = "".join([opt["result-dir"],  test + suffix+ ".result"])  

        opt["record-file"] = "".join([opt["result-dir"], "/", test + suffix + ".record"])

        retcode = 0
        output = ""
        errput = ""
        my_retcode = 0
        my_output = ""
        my_errput = ""
        ob_retcode = 0
        ob_output = ""
        ob_errput = ""
        connector = 'ob'

        if "my_host" in opt or "oracle_host" in opt:
            # entre compare mode
            temp_opt=dict(opt)
            temp_opt["record"]=True
            if "my_host" in opt:
              connector = 'mysql'
              temp_opt["host"]=opt["my_host"]
              temp_opt["port"]=opt["my_port"]
              temp_opt["mysql_mode"] = True
              temp_opt["user"]=opt["my_user"]
              temp_opt["password"]=opt["my_password"]
            else:
              connector = 'oracle'
              temp_opt["host"]=opt["oracle_host"]
              temp_opt["port"]=opt["oracle_port"]
              temp_opt["mysql_mode"] = False
              temp_opt["user"]=opt["oracle_user"]
              temp_opt["password"]=opt["oracle_password"]
              temp_opt["database"]=opt["oracle_database"]

            if "record" in opt and opt["record"]:
                 temp_opt["record-file"] = join(temp_opt["result-dir"], test + ".result")
            else:
                 temp_opt["record-file"] = join(temp_opt["result-dir"], test + ".result.my")
            java_opt = ''
            temp_opt["connector"] = connector
            my_cmd = self.find_bin(temp_opt) + str(Arguments(temp_opt)) + java_opt;
            # print my_cmd

            try:
                my_p = Popen(shlex.split(my_cmd), stdout = PIPE, stderr = PIPE)
                my_output, my_errput = my_p.communicate()
                my_retcode = my_p.returncode
            except Exception as e:
                my_errput = e;
                my_retcode = 255;
            if my_retcode != 0:
                my_errput = connector + "<<< " + my_errput
                return {"name" : test, "ret" : my_retcode, "output" : my_output,"cmd" : my_cmd, "errput": my_errput}

            opt["record"] = False
            opt["result-file"] = temp_opt["record-file"]
            if "only" in opt:
                return {"name" : test, "ret" : my_retcode, "output" : my_output,"cmd" : my_cmd, "errput": my_errput}


        os.putenv('TENANT', opt['case_mode'])
        os.putenv('OBMYSQL_USR', str(opt["user"] + "@" + opt['case_mode']))
        if "java" in opt:
          opt["connector"] = "ob"
        cmd = self.find_bin(opt) + str(Arguments(opt))

        is_opt_case = check_tag_exists(opt["test-file"], r"#[ \t]*tags[ \t]*:", "optimizer")
        if is_opt_case:
          self.call_sql_scripts("mysql_test/include/explain_internal_init.inc", tout, opt)
        try:
            p = Popen(shlex.split(cmd), stdout = PIPE, stderr = PIPE)
            try:
                output, errput = p.communicate(timeout=tout)
                retcode = p.returncode
            except TimeoutExpired as e:
                p.terminate()
                output = None
                retcode = 1
                if "source-limit" in opt and "g.buffer" in opt["source-limit"]:
                    errput = "%s secs out of soft limit (%s secs), sql may be hung, please check" % (opt["source-limit"]["g.buffer"], tout)
                else:
                    errput = "%s seconds timeout, sql may be hung, please check" % tout
            if isinstance(errput, str):
                errput = errput.decode("utf-8", "replace")
        except Exception as e:
            errput = str(e);
            retcode = 255;
        if is_opt_case:
          self.call_sql_scripts("mysql_test/include/explain_internal_end.inc", tout, opt)

        return {"name" : test, "ret" : retcode, "output" : output, "cmd" : cmd, "errput" : errput}

class Manager:
    test_set = None
    test = None
    opt = None
    before_one = None
    after_one = None
    log_fp = None
    case_index = None
    exec_index = None
    stop = False
    reboot = True
    run_time = None
    json_ob = []
    result_t_dir = None

    def __init__(self, opt):
        '''Check and autofill "opt" before run a "Tester".
        Check list contains "test-dir", "result-dir", "record-dir"
        '''
        if "test-dir" in opt:
            opt["test-dir"] = join(mtdir, opt["test-dir"])
        else:
            opt["test-dir"] = join(mtdir, "t")
        
        if "result-dir" in opt:
            opt["result-dir"] = join(mtdir, opt["result-dir"] + '/')
        else:
            opt["result-dir"] = join(mtdir, "r")

        if "oblog_diff" in opt:
            opt["oblog_diff"] = True
            if "oblog-test-dir" in opt:
                opt["test-dir"] = join(mtdir, opt["oblog-test-dir"])
            if "oblog-result-dir" in opt:
                opt["result-dir"] = join(mtdir, opt["oblog-result-dir"] + '/')

        if "record-dir" in opt:
            opt["record-dir"] = join(mtdir, opt["record-dir"] + '/')
        else:
            opt["record-dir"] = join(mtdir, "r")

        if "log-dir" in opt:
            opt["log-dir"] = join(mtdir, opt["log-dir"])
        else:
            opt["log-dir"] = join(mtdir, "log")
        if "tmp-dir" in opt:
            opt["tmp-dir"] = join(mtdir, opt["tmp-dir"])
        else:
            opt["tmp-dir"] = join(mtdir, "tmp")
        if "var-dir" in opt:
            opt["var-dir"] = join(mtdir, opt["var-dir"])
        else:
            opt["var-dir"] = join(mtdir, "var")
        if "profile" in opt:
            opt["profile"] = True
            opt["test-dir"] = join(mtdir, 'p')
        if "suite" in opt:
            opt["test-dir-suite"] = [join(mtdir, "test_suite/"+dir+"/t") for dir in opt["suite"].split(',')]
            opt["result-dir-suite"] = [join(mtdir, "test_suite/"+dir+"/r/") for dir in opt["suite"].split(',')]


        my_mkdir(opt["record-dir"])
        my_mkdir(opt["log-dir"])

        self.result_t_dir = opt["result-dir"]
        self.opt = opt

        self.run_time = strftime("%Y-%m-%d %X")

    def check_tests(self):
        test_set=[]
        has_test_point = False;

        if "suite" not in self.opt:
            if "test-set" in self.opt:
                 self.test_set = self.opt["test-set"]
                 has_test_point = True
            else:
                 if not "test-pattern" in self.opt:
                      self.opt["test-pattern"] = "*.test"
                 else:
                      has_test_point = True;
                 pat = join(self.opt["test-dir"], self.opt["test-pattern"])
                 self.test_set = [basename(test).rsplit('.', 1)[0] for test in glob.glob(pat)]
        else:
            has_test_point = True
            for dir in self.opt["test-dir-suite"]:
                 suitename=basename(dirname(dir))
                 if "test-set" in self.opt:
                      test_set_tmp=[suitename+"."+ test for test in self.opt["test-set"]]
                 else:
                      if not "test-pattern" in self.opt:
                           self.opt["test-pattern"] = "*.test"
                      pat = join(dir, self.opt["test-pattern"])
                      test_set_tmp =[suitename+"."+basename(test).rsplit('.', 1)[0] for test in glob.glob(pat)]

                 test_set.extend(test_set_tmp)
            self.test_set=test_set
            if "all" in self.opt and  self.opt["all"] == "all":
                 pat= join(mtdir, "t/*.test")
                 test_set_t = [basename(test).rsplit('.', 1)[0] for test in glob.glob(pat)]
                 self.test_set.extend(test_set_t)
        if "keyword" in self.opt:
            try:
                 cmd_t="grep -Eil '"+self.opt['keyword']+"'"+" mysql_test/t/*.test|awk -F'/' '{print $3}'|awk -F'.' '{print $1}'"
                 p = Popen(cmd_t, stdout = PIPE, stderr = PIPE,shell=True)
                 output, errput = p.communicate()
                 testlist_t=output.strip().split('\n')
                 cmd_suite="grep -Eil '"+self.opt['keyword']+"'"+" mysql_test/test_suite/*/t/*.test|awk -F '/' '{print $3,$5}'|awk -F '.' '{print $1}'|tr -t ' ' '.'"
                 p_suite=Popen(cmd_suite, stdout=PIPE, stderr=PIPE, shell=True)
                 output, errput = p_suite.communicate()
                 testlist_suite=output.strip().split('\n')
                 test_set=testlist_t+testlist_suite
                 self.test_set = (has_test_point and (list(set(self.test_set).intersection(set(test_set))),) or (list(set(test_set)),))[0]
                 has_test_point = True
            except Exception as e:
                 print "[Error] %s there is some error in your grep tag" % e
                 raise

        if "nogroup" in self.opt:
            try:
                 cmd_t="grep -EiL 'owner\s+group\S*\s*:'+"+" mysql_test/t/*.test|awk -F'/' '{print $3}'|awk -F'.' '{print $1}'"
                 p = Popen(cmd_t, stdout = PIPE, stderr = PIPE,shell=True)
                 output, errput = p.communicate()
                 testlist_t=output.strip().split('\n')
                 cmd_suite="grep -EiL '"+'owner\s+group\S*\s*:'+"'"+" mysql_test/test_suite/*/t/*.test|awk -F '/' '{print $3,$5}'|awk -F '.' '{print $1}'|tr -t ' ' '.'"
                 p_suite=Popen(cmd_suite, stdout=PIPE, stderr=PIPE, shell=True)
                 output, errput = p_suite.communicate()
                 testlist_suite=output.strip().split('\n')
                 test_set=testlist_t+testlist_suite
                 self.test_set = (has_test_point and (list(set(self.test_set).intersection(set(test_set))),) or (list(set(test_set)),))[0]
                 has_test_point = True
            except Exception as e:
                 print "[Error] %s there is some error in your grep tag" % e
                 raise

        if "owner-group" in self.opt:
             test_set = [test_name(test) for test in find_tag_tests(r"#[ \t]*owner group[ \t]*:", self.opt["owner-group"])]
             self.test_set = (has_test_point and (list(set(self.test_set).intersection(set(test_set))),) or (list(set(test_set)),))[0]
             has_test_point = True
        if "owner" in self.opt:
             test_set = [test_name(test) for test in find_tag_tests(r"#[ \t]*owner[ \t]*:", self.opt["owner"])]
             self.test_set = (has_test_point and (list(set(self.test_set).intersection(set(test_set))),) or (list(set(test_set)),))[0]
             has_test_point = True
        if "test-tags" in self.opt:
             global test_tags
             test_tags = list(set(test_tags).union(set(listdir(join(mtdir, "test_suite")))))
             diff_tags = list(set(self.opt["test-tags"]).difference(set(test_tags)))
             if (len(diff_tags) > 0):
               print '\nERROR: %r not in test_tags\n' % diff_tags
               test_set = []
             else:
               test_set = [test_name(test) for test in find_tag_tests(r"#[ \t]*tags[ \t]*:", self.opt["test-tags"])]
             self.test_set = (has_test_point and (list(set(self.test_set).intersection(set(test_set))),) or (list(set(test_set)),))[0]
             has_test_point = True

        # exclude somt tests.
        if "exclude" not in self.opt:
            self.opt["exclude"] = None
        self.test_set = filter(lambda k: k not in self.opt["exclude"], self.test_set)
        if "filter" in self.opt:
            if self.opt["filter"]in["c","cp","j","jp","o","op","slave","proxy"]:
                exclude_list =get_exclude_case_by_base_name(self.opt["filter"])
            else:
                exclude_list = []
            self.test_set = filter(lambda k: k not in exclude_list, self.test_set)
            #
        #print encodedjsonprint self.test_set
        ##Reorder when there is an all parameter, ensuring the order of running cases
        if "all" in self.opt and self.opt["all"] == 'all':
            test_set_suite = filter(lambda k: '.' in k, self.test_set)
            test_set_suite = sorted(test_set_suite)
            test_set_t = filter(lambda k: k not in test_set_suite, self.test_set)
            self.test_set = sorted(test_set_t)
            self.test_set.extend(test_set_suite)
            if "succ" in self.opt and self.opt["succ"] == "succ":
               self.test_set = filter(lambda k: k not in succ_filter, self.test_set)
        else:
            self.test_set = sorted(self.test_set)
            
        if 'slices' in self.opt and 'slice_idx' in self.opt:
            slices = int(self.opt['slices'])
            slice_idx = int(self.opt['slice_idx'])
            if "slb_id" not in self.opt:
                self.test_set = self.test_set[slice_idx::slices]
        if self.opt['mode'] == 'oracle':
           not_run = '_mysql'
           #self.test_set = filter(lambda k: not k.endswith(not_run), self.test_set)
           self.test_set = filter(lambda k: k.endswith('_oracle'), self.test_set)
        if self.opt['mode'] == 'mysql':
           not_run = '_oracle'
           self.test_set = filter(lambda k: not k.endswith(not_run), self.test_set)
            
        return self.test_set

    def find_owner(self, testfile):
      owner = "anonymous"
      try:
        cmd_t="grep -E 'owner\s*:' " + testfile + " | awk -F':' '{print $2}'"
        p = Popen(cmd_t, stdout = PIPE, stderr = PIPE,shell=True)
        output, errput = p.communicate()
        owner = output.decode("utf-8").strip()
      except Exception as e:
        print("fail open %s" % (testfile))
      return owner
    def slb_lock(self, case):
        slb_data = {'eid':self.opt['slb_id'], 'case':case}
        qp = urllib.urlencode(slb_data)
        url = "http://" + self.opt['slb_host'] + "/farm/mysqltest/recorder/lock.php?" + qp
        try:
            rs = urllib.urlopen(url)
            if rs.code == 200:
                return 1
            else:
                return 0
        except Exception, e:
            print("lock case failed, since %s" % e)
        finally:
            if rs:
                rs.close()

    def slb_succ(self, case):
        slb_data = {'eid':self.opt['slb_id'], 'case':case}
        qp = urllib.urlencode(slb_data)
        url = "http://" + self.opt['slb_host'] + "/farm/mysqltest/recorder/success.php?" + qp
        try:  
            rs = urllib.urlopen(url)
            if rs.code != 200:
                print("mark case status failed, status code: %d" % rs.code)
        except Exception, e:
            print("mark case status failed, since %s" % e)
        finally:
            if rs:
                rs.close()

    def result_stat(self):
        ret = ""
        total = len(self.result)
        succ = len(filter(lambda item: item["ret"] == 0, self.result))
        fail = total - succ
        ret += "\n<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"
        ret += "success %d out of %d\n" % (succ, total)
        ret += "fail tests list:\n"
        for line in [ "%-12s: %s\n" % (item["name"], item["errput"]) for item in self.result ]:
            ret += line
        ret += ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n"
        return ret
    def run_one(self, test):
        self.case_index += 1
        suite_name=''
        test_ori = test;
        self.log_fp.write('============================================================\n')
        if 'slb_id' in self.opt:
#            pslb('try lock case no.%d [ %s ]' % (self.case_index, test))
            if self.slb_lock(test_ori):
                self.exec_index +=1
                pslb('case no.%d [ %s ] locked success' % (self.case_index, test))
#                pslb('case locked success...')
            else:
#                pslb('case locked by other slots, skip...')
                return None

        self.log_fp.write('%s INFO: [ %s ] case start!\n' % (strftime("%Y-%m-%d %X"), test))
        self.test = test
        run_idx = 0
        retry_msg = ""
        if test in reboot_cases:
            self.reboot = True
        opt = dict(self.opt)
        test_dir = opt["test-dir"]
        result_dir = opt["result-dir"]
        if len(test.split('.')) == 2:
            suite_name = test.split('.')[0]
            test_dir = join(mtdir, "test_suite/%s/t/" % suite_name)
            result_dir = join(mtdir, "test_suite/%s/r/" % (suite_name))
            test = test.split('.')[1]
        self.owner = self.find_owner(join(test_dir, test + ".test"))

        soft = 3600
        buffer = 0
        if "source-limit" in opt:
            if test_ori in opt["source-limit"]:
                soft = opt["source-limit"][test_ori]
            elif "g.default" in opt["source-limit"]:
                soft = opt["source-limit"]["g.default"]

            if "g.buffer" in opt["source-limit"]:
                buffer = opt["source-limit"]["g.buffer"]

        case_timeout = soft + buffer

        while run_idx < 2:
            opt["test-dir"] = test_dir
            opt["result-dir"] = result_dir
            if run_idx > 0:
                self.log_fp.write('%s INFO: [ %s ] case auto-retry !\n' % (strftime("%Y-%m-%d %X"), test))
                retry_msg = "in auto retry mode"
                self.reboot = True
            if self.opt['auto-retry']:
                run_idx = run_idx + 1
            else:
                run_idx = 9999
            if self.before_one:
                self.before_one(self)
            if 'slb_id' in self.opt:
                prun("%s [ %d / %d ] (%s) slb no.%d %s" % (test_ori, self.case_index, len(self.test_set), self.owner, self.exec_index, retry_msg))
            else:
                prun("%s [ %d / %d ] (%s) %s" % (test_ori, self.case_index, len(self.test_set), self.owner, retry_msg))

            start = time()
            try:
                result = Tester().run(test, case_timeout, opt)
            except Exception as e:
                print("%s, please check" % e)
                raise
            during = time() - start;
            result["start_ts"] = strftime("%F %X")
            result["cost_time"] = during
            result["branch"] = opt["OB_BRANCH"]
            result["type"] = "mysqltest"
            result["subtype"] = opt["filter"]
            result["test"] = test_ori

            if 'profile' in opt:
                content = read(opt['record-file'] + '.profile')
                print content
            # For slave mode and java mode, ignore the comparison of explain results
            patterns = ['output','NAME', 'SORT','SCAN','LIMIT','EXCHANGE','GET','FUNCTION','MERGE','JOIN','MATERIAL','DISTINCT','SUBPLAN','UNION|ALL','EXPRESSION','SCALAR','HASH','VALUES','DELETE','result','reject','=====','-------','conds','output','access','GROUP','DELETE','UPDATE','INSERT','CONNECT','nil','values','COUNT','^$']
            count = 0
            match = 0
            # Do not process the result comparison of liboblog
            if (re.search("liboblog_r",result["errput"])):
                print "do nothing for liboblog"
            elif ((opt['filter'] == 'slave' and opt ['slave-cmp'] == 1) or opt['filter'] == 'j' or opt['filter'] == 'jp'):
                diff = result["errput"].split('\n')
                for line in diff:
                    match = 0
                    if re.search("^\+",line) or re.search("^\-",line):
                        for pattern in patterns:
                            if re.search(pattern, line):
                                match = match + 1
                                continue 
                        if match == 0:
                            count = count + 1
                            break
                if count == 0:
                    #Handle the situation where the result file does not exist in java mode
                    if re.search("\+",result["errput"]):
                        result["ret"] = 0
                     
            if result["ret"] == 0:
                self.log_fp.write('%s INFO: [ %s ] case success!\n' % (strftime("%Y-%m-%d %X"), result["name"]))
                if len(suite_name) == 0:
                     psucc("%s ( %f s )" % (test, during))
                else:
                     psucc("%s ( %f s )" % (suite_name+'.'+test, during))
                if during > soft:
                     pwarn("case cost %s more secends than soft limit, please let the owner know." % int(during - soft + 1))
                if 'slb_id' in self.opt:
                    self.slb_succ(test_ori)
                self.reboot = False
                #break auto retry loop
                break
            else:
                self.log_fp.write('%s INFO: [ %s ] case failed!\n' % (strftime("%Y-%m-%d %X"), result["name"]))
                if  result["errput"] == None:
                    result["errput"] = ''
                self.log_fp.write("the diff is %s\n" % result["errput"].strip().encode('utf-8'))
                if len(suite_name) == 0:
                    pfail("%s ( %f s )\n%s" % (test, during, shrink_errmsg(result["errput"])))
                else:
                    pfail("%s ( %f s )\n%s" % (suite_name+'.'+test, during, shrink_errmsg(result["errput"])))
                self.reboot = True
        if self.after_one:
            self.after_one(self, result)
        if 'obfarm' in opt:
            self.record_tc(test_ori, opt, result)
        return result

    def record_tc(self, test, opt, result):

        tc_info = dict()
        if opt['role'] == 'proxy0':
            tc_info['type'] = 'mysqltest ' + self.opt["filter"] + 'proxy'
        elif opt['role'] == 'obs1':
            tc_info['type'] = 'mysqltest ' + self.opt["filter"] + 'slave'
        else:
            tc_info['type'] = 'mysqltest ' + self.opt["filter"]
        tc_info['name'] = test
        tc_info['status'] = result["ret"]
        # The following 16000 is an experienc value for max length of msg
        if result["errput"] != None:
            tc_info['msg'] = result["errput"][0:16000]
        else:
            tc_info['msg'] = ''
        cmd = "md5sum " + opt['test-file']
        p = Popen(shlex.split(cmd), stdout = PIPE, stderr = PIPE)
        output, errput = p.communicate()
        #tc_info['md5'] = output[1:32]
        #tc_info['svn'] = self.opt["OB_VERSION"][0:5]
        ob_ver = opt["OB_VERSION"]
        if len(ob_ver) < 10:    # svn
            tc_info['revision'] = opt["OB_VERSION"][0:5]
        else:   # git
            try:
                index = opt["OB_VERSION"].index("local")
                tc_info['revision'] = opt["OB_VERSION"][index:]
            except Exception as e:
                print e
                tc_info['revision'] = opt["OB_VERSION"]
        tc_info['branch'] = opt["OB_BRANCH"]
        tc_info['cost_time'] = result["cost_time"]
        tc_info['date'] = self.run_time
        localIP = socket.gethostbyname(socket.gethostname())
        obfarm_dir = abspath('.')
        log_address = "%s:%s/collected_log" % (localIP, obfarm_dir)
        tc_info['log_address'] = log_address
        tc_info['major_fail'] = result.get("major_fail", 0)
        self.json_ob.append(tc_info)
        #print self.json_ob


    def save_public_vars(self):
        output={}
        output["public_vars"] = "This is a flag for public vars."
        # Add BUILD_URL from jenkins
        p = Popen("echo $BUILD_URL", shell=True, stdout = PIPE, stderr = PIPE)
        out, _ = p.communicate()
        output["build_url"] = out.strip()
        # Add release version
        cmd = 'bin/rootserver  --version 2>&1  |grep "rootserver (OceanBase"|sed "s#rootserver (OceanBase ##g"|sed "s#\.e.*##g"'
        p = Popen(cmd, shell=True, stdout = PIPE, stderr = PIPE)
        release, _ = p.communicate()
        output["release"] = release.strip()
        return output


    def start(self):
        def is_passed():
            return len(filter(lambda x: x["ret"] != 0, self.result)) == 0
        self.check_tests()
        log_file = join(self.opt["log-dir"], \
                        (self.opt["log-temp"] or "mysqltest-%s.log") %\
                        strftime("%Y-%m-%d_%X"))

        runmode = "record" in self.opt and self.opt["record"] and "Record" or "Test"
        engine_str = "old"
        if self.opt["engine"] == 'new':
            engine_str = "new"
        pheader("Running %d cases ( %s Mode ) ( %s ) ( engine = %s )" % (len(self.test_set),
                                                                    runmode,
                                                                    self.opt["filter"],
                                                                    engine_str))
        pinfo(strftime("%F %X"))
        try:
            self.log_fp = open(log_file, "w")
            self.case_index = 0
            self.exec_index = 0
            self.result = [ self.stop or self.run_one(test) for test in self.test_set ]
            self.result = filter(lambda item: type(item) == dict, self.result);
            self.log_fp.write(self.result_stat().encode("utf-8"))
        except Exception as e:
            raise
        finally:
            self.log_fp.close()

        # upload json result if specified
        def upload_mysqltest_results(ip , filename):
            response = None
            try:
                obfarm_upload_mysqltest_results_url = "http://" + ip + "/obfarm/obtest/results/mysqltest/"
                req = urllib2.Request(obfarm_upload_mysqltest_results_url)
                connection = httplib.HTTPConnection(req.get_host())
                connection.request('POST', req.get_selector(), file(filename).read())
                response = connection.getresponse()
            except Exception, e:
                print str(e)
                print 'The connection info specified for obfarm uploading is not accessible.'
                print 'url: ' + obfarm_upload_mysqltest_results_url
            finally:
                connection.close()
            if None != response:
                return response.read()
            else:
                return None
        if 'obfarm' in self.opt:
            # add public vars into json_ob
            public_vars = self.save_public_vars()
            self.json_ob.append(public_vars)
            # save json object
            json_fp = open(self.opt["log-dir"] + "/mysqltest.json", "w")
            json.dump(self.json_ob, json_fp)
            json_fp.close()
            obfarm_ips = self.opt["obfarm"].split(",")
            for obfarm_ip in obfarm_ips:
                upload_mysqltest_results(obfarm_ip , self.opt["log-dir"] + "/mysqltest.json")

        passcnt = len(filter(lambda x: x["ret"] == 0, self.result))
        totalcnt = len(self.result)
        failcnt = totalcnt - passcnt
        slbinfo=''
        if 'slb_id' in self.opt:
            slbinfo=', %d tests skiped since slb' % (len(self.test_set) - len(self.result))
        pheader("%d tests run done%s!" % (len(self.result), slbinfo))
        if is_passed():
            ppass("%d tests" % len(self.result))
        else:
            pfail("%d tests are failed out of %s total" % (failcnt, totalcnt))
            passlst,faillst='',''
            for i,t in enumerate(self.result):
                if t["ret"] == 0:
                    passlst=t["test"] if passlst == '' else (passlst+','+t["test"])
                else:
                    faillst=t["test"] if faillst == '' else (faillst+','+t["test"])
            ppasslst(passlst)
            pfaillst(faillst)

        return self.result

if __name__ == '__main__':
    mgr = Manager(opt)
    mgr.set_tests(["bool", "default", "bigint", "compare"])
    mgr.start()

__all__ = ["Manager", "pinfo", "prun", "pfail", "psucc", "shrink_errmsg"]

# Local Variables:
# time-stamp-line-limit: 1000
# time-stamp-start: "Last change:[ \t]+"
# time-stamp-end: "[ \t]+#"
# time-stamp-format: "%04y-%02m-%02d %02H:%02M:%02S"
# End:
