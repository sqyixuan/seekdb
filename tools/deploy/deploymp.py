#!/usr/bin/env python2.6
'''
Usages:
./deploymp.py ob{1..n}.reboot
./deploymp.py ob{1,2,3,4}.reboot
./deploymp.py  ob{1..n}.mysqltest [testset=...]
./deploymp.py  ob{1..n}.smalltest
./deploymp.py  ob{1..n}.smalltest para=num
./deploymp.py  ob{1..n}.succtest
./deploymp.py  ob{1..n}.minitest
./deploymp.py  ob{1..n}.print info
'''

import sys, os
_base_dir_ = os.path.dirname(os.path.realpath(__file__))
sys.path.extend([os.path.join(_base_dir_, path) for path in '.'.split(':')])
import re
import glob
import subprocess
import random
import inspect
import signal
import mysql_test
import time
from pprint import pprint, pformat
from os.path import  join,realpath
from os import listdir
from time import strftime
_base_dir_ = os.path.dirname(os.path.realpath(__file__))
sys.path.extend([os.path.join(_base_dir_, path) for path in '.'.split(':')])


def usages():
    sys.stderr.write(__doc__)
    sys.exit(1)

def killpid(logfile):
    cmd="pkill -9 -u `whoami`  -f 'deploy.py ' 2>&1 >/dev/null"
    os.system(cmd)
    cmd="pkill -9 -u `whoami`  -f 'hap.py ' 2>&1 >/dev/null"
    os.system(cmd)
    cmd="pkill -9 -u `whoami`  -f tail -f cat " + logfile + "plog 2>&1 >/dev/null"
    os.system(cmd)

def popen(cmd, cwd=None):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd)
    output = p.communicate()[0]
    return output

def parse_cmd_args(args):
    return [i for i in args if not re.match('^\w+=', i)], dict(i.split('=', 1) for i in args if re.match('^\w+=', i))

def parse_mysqltest_args(args,opt):
    if "testpat" in args:
        if "test-set" in args:
            del opt["test-set"]
        opt["test-pattern"] = ob["testpat"]
    if "group" in args:
         opt["owner-group"] = [group.strip() for group in args["group"].split(',')]
    if "owner" in args:
         opt["owner"] = [owner.strip() for owner in args["owner"].split(',')]
    if "tags" in args:
         opt["test-tags"] = [tag.strip() for tag in args["tags"].split(',')]
    if "suite" in args:
         opt["suite"] = args["suite"].strip()
    if method == "succtest":
         opt["succ"] = "succ"
         opt["all"] = 'all'
         opt["suite"] = ','.join(listdir(realpath("mysql_test/test_suite")))
    if "all" in args:
         opt["all"] = 'all'
         opt["suite"] = ','.join(listdir(realpath("mysql_test/test_suite")))
    if "keyword" in args:
         opt["keyword"]=args['keyword']
    if 'nogroup' in args:
         opt["nogroup"]=True
    if "tags" in args:
            opt["test-tags"] = [tag.strip() for tag in args["tags"].split(',')]
    if 'mode' in kw_args:
            opt['mode'] = args['mode']
    else:
            opt['mode'] = 'both'
    return opt

if __name__ == '__main__':
    print strftime("%Y-%m-%d %X")
    list_args, kw_args = parse_cmd_args(sys.argv[1:])
    list_args or usages()
    # gcfg['_dryrun_'] = kw_args.get('_dryrun_') == 'True'
    # gcfg['_verbose_'] = kw_args.get('_verbose_') == 'True'
    # gcfg['_quiet_'] = kw_args.get('_quiet_') == 'True'
    # _loop_ = int(kw_args.get('_loop_', '1'))
    method_conf=''
    if 'logfile' in kw_args:
        logfile = kw_args['logfile']
        del kw_args['logfile']
    else:
        logfile = 'multip'
   
    method, rest_args = list_args[0], list_args[1:]
    my_args=[i for i in list_args if not re.match('disable-reboot|ps|java|testpat=\S+|testset=\S+|suite=\S+|only|record|collect=\S+|collect_all|group=\S+|owner=\S+|tags=\S+|keyword=\S+',i)]
    mytest_args=[i for i in kw_args if  re.match('testpat=\S+|testset=\S+|suite=\S+|group=\S+|owner=\S+|tags=\S+|keyword=\S+',i)]
    if len(method.split(":")) == 2:
        method_conf=method.split(":")[0]
        method=method.split(":")[1]
        obi_list = [i.split(":")[1] for i in my_args]
    else:
        obi_list = list_args
    method = method.split('.')[1:]
    if len(method) == 1:
        method = method[0]
    elif len(method) > 1:
        method=".".join(method)
    rest_args=filter(lambda k: k not in my_args, rest_args)
    rest_args=" ".join(rest_args)
    kw_args_tmp=kw_args.copy()
    kw_args_tmp_str=''
    if "testset" in kw_args_tmp:
        del kw_args_tmp['testset']
    for key in kw_args_tmp:
        kw_args_tmp_str=kw_args_tmp_str+ " "+ key + "="+kw_args_tmp[key]

    if method != 'printlog' :
        killpid(logfile)

    if "para" in kw_args:
        para_num=int(kw_args['para'])
    else:
        para_num=len(my_args)

    if para_num == 0:
        sys.stderr.write("error,please check your parameter")
        usages()
        sys.exit(1)


    my_num=len(my_args)
    if my_num < para_num:
        sys.stderr.write("error,para_num must not large obi num")
        usages()
        sys.exit(1)

    if my_num > para_num:
        my_args_para=random.sample(my_args,para_num)

    if my_num == para_num:
        my_args_para=my_args
    def printinfolog():
        cmd_info="cat"
        time.sleep(1)
        for i in range(0,para_num):
            obi_name=obi_list[i].split('.')[0]
            info_log=logfile + 'log_'+obi_name
            cmd_info=cmd_info +" " +info_log
            output = popen(cmd_info)
            print output
    def print_test_info():
        time.sleep(3)
        file_read=open(logfile + 'resultall','r')
	print "#################Overall Information is:##################"
        pattern=re.compile(r'mysqltest$|FAILED')
	for line in file_read:
            line=line.strip()
            matchobj = pattern.search(line)
	    if matchobj:
                 if  len(line) > 1:
                      print line
    try:
        if method !="printlog":
            if  re.match('\S+test',method):
                 if "testset" in kw_args:
                      all_test_set_pre = kw_args['testset'].strip().split(',')
                 elif method == "smalltest":
                      all_test_set_pre =  mysql_test.small_test
                 elif re.match("\S*psmalltest",method):
                      all_test_set_pre =  mysql_test.psmall_test
                 elif method == "minitest":
                      all_test_set_pre =  mysql_test.mini_test
                 else:
                      opt = dict(mysql_test.opt)
                      opt = parse_mysqltest_args(kw_args,opt)
                      mgr = mysql_test.Manager(opt)
                      all_test_set_pre = mgr.check_tests()
                 func = lambda x,y:x if y in x else x + [y]
                 all_test_set=reduce(func,[[],]+all_test_set_pre)
                 case_num=len(all_test_set)
                 step=case_num/para_num
                 all_test_set2=[None]*para_num
                 mycase=[]
            cmd_result=""
            for i in range(0,para_num):
                 m=my_args_para[i]
                 obi_name=obi_list[i].split('.')[0]
                 my_log=logfile + 'log_'+obi_name
                 cmd_result=cmd_result +" " +my_log
                 if  re.match('\S+test',method):
                      my_case=(','.join(all_test_set[i:case_num:para_num]))
                      cmd = "./deploy.py {0} testset={1} {2} {3} >{4} 2>&1 &".format(m,my_case,rest_args,kw_args_tmp_str,my_log)
                 else:
		      cmd = "./deploy.py {0} {1} {2} >{3} 2>&1 &".format(m,rest_args,kw_args_tmp_str,my_log)
                 print cmd
                 result=os.system(cmd)
            cmd_ps='ps -ef|grep -E "test testset=|reboot"|grep deploy.py|grep -v grep|grep -v deploymp.py|wc -l'
            cmd_tail='tail -f ' + cmd_result
            popen_tail = subprocess.Popen(cmd_tail, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            num=int(popen(cmd_ps).strip())
            while num != 0:
                 num=int(popen(cmd_ps).strip())
                 print(popen_tail.stdout.readline().strip())
	    if not re.match('\S+test',method):
                printinfolog()
	    else:
	        cmd_result="cat " + cmd_result + " > " + logfile + "resultall"
                subprocess.Popen(cmd_result, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                print_test_info()
            print strftime("%Y-%m-%d %X") +" ################# TEST END ##############"


        if method =="printlog":
	    printinfolog()
    except Exception as e:
            print e
            sys.exit(1)
