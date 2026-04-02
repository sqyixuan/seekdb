#!/usr/bin/env python2

"""
Usage:
  ./run_mysqltest.py {pull | compile | copy | reboot | test} server case [opts]
Example:
  ./run_mysqltest.py pull ob1 datatype.dml record
  means:
  pull -> compile -> copy -> ob1.reboot -> ob1.mysqltest testset=datatype.dml disable-reboot record
ps:
  disable-reboot is always on.
"""

import sys
import subprocess
import getopt
from run_test_config import *
from run_test_base import RunTestBase
from run_test_base import run_cmd

def my_help():
  print __doc__

class RunMysqlTest(RunTestBase):

  def __init__(self, src_root, compile_root, head_job, server, case, agrs):
    RunTestBase.__init__(self, src_root, compile_root, head_job, server, [self.JOB_TEST])
    self.case = case
    self.agrs = agrs

  def run(self):
    ret = 0
    if ret == 0:
      ret = self.start_server()
    if ret == 0:
      ret = run_cmd("./hap.py %s.mysqltest testset=%s disable-reboot %s" % (self.server, self.case, " ".join(self.agrs)))
    return ret

  JOB_TEST = "test"
  case = ""
  agrs = ""

if __name__ == '__main__':
  ret = 0;
  src_root = get_src_root()
  compile_root = get_compile_root()
  head_job = "compile"
  server = ""
  case = ""
  opts = ""

  # get all opts and args.
  opts, args = getopt.getopt(sys.argv[1:], "j:s:c:h")
  for name, value in opts:
    if name in ["-j"]:
      head_job = value
    elif name in ["-s"]:
      server = value
    elif name in ["-c"]:
      case = value
    elif name in ["-h"]:
      my_help()

  test = RunMysqlTest(src_root, compile_root, head_job, server, case, args)
  ret = test.run()
  sys.exit(ret)
