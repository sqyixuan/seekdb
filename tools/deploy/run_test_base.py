#!/usr/bin/env python2

"""
Usage:
  ./run_test_base.py -j job -s server
  job: pull / compile / copy / reboot.
       default compile.
Example:
  ./run_test_base.py -j pull -s ob1
means:
  pull -> compile -> copy -> ob1.reboot.
"""

import sys
import subprocess
import getopt
from run_test_config import *

def run_cmd_result(cmd):
  print "**** run cmd ****: %s" % (cmd)
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  ret = p.wait()
  outputs = ""
  if ret == 0:
    ret = p.returncode
    outputs = p.stdout.readlines()
  return ret, outputs

def run_cmd(cmd):
  print "**** run cmd ****: %s" % (cmd)
  return subprocess.call(cmd, shell=True)

def run_sql(sql, server):
  return run_cmd("echo \"%s\" | ./hap.py %s.sql" % (sql, server))

def my_help():
  print __doc__

class RunTestBase(object):

  def __init__(self, src_root, compile_root, head_job, server, job_list = []):
    self.src_root = src_root
    self.compile_root = compile_root
    self.head_job = head_job
    self.server = server
    self.job_list = self.job_list + job_list
    return

  def __init(self):
    ret = 0
    if self.head_job in self.job_list:
      self.head_job_idx = self.job_list.index(self.head_job)
    else:
      ret = -1
      my_help()
    return ret

  def start_server(self):
    ret = 0
    if ret == 0:
      ret = self.__init()
    if ret == 0 and self.need_do_job(self.JOB_PULL):
      ret = run_cmd("cd %s/ && git pull" % (self.src_root))
    if ret == 0 and self.need_do_job(self.JOB_COMPILE):
      ret = run_cmd("cd %s/src/ && %s/ob-make observer/observer" % (self.compile_root, self.src_root))
    if ret == 0 and self.need_do_job(self.JOB_COPY):
      ret = run_cmd("./copy.sh %s/" % (self.compile_root))
    if ret == 0 and self.need_do_job(self.JOB_REBOOT):
      ret = run_cmd("./hap.py %s.reboot" % (self.server))
    return ret

  def need_do_job(self, cur_job):
    return self.head_job_idx <= self.job_list.index(cur_job)

  JOB_PULL = "pull"
  JOB_COMPILE = "compile"
  JOB_COPY = "copy"
  JOB_REBOOT = "reboot"
  
  src_root = ""
  compile_root = ""
  head_job = ""
  server = ""
  job_list = [JOB_PULL, JOB_COMPILE, JOB_COPY, JOB_REBOOT]
  head_job_idx = -1

if __name__ == '__main__':
  ret = 0
  src_root = get_src_root()
  compile_root = get_compile_root()
  head_job = "compile"
  server = ""

  # get all options and args.
  opts, args = getopt.getopt(sys.argv[1:], "j:s:h")
  for name, value in opts:
    if name in ["-j"]:
      head_job = value
    elif name in ["-s"]:
      server = value
    elif name in ["-h"]:
      my_help()

  test = RunTestBase(src_root, compile_root, head_job, server)
  ret = test.start_server()
  sys.exit(ret)
