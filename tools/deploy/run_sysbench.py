#!/usr/bin/env python2

"""
Usage:
  ./run_sysbench.py -j job -s server -c case [-t thread] [-p percent]
  job: pull / compile / copy / reboot / trace.
       default compile.
  case: get / insert / update / tcbuyer / tcbuyer_select.
  thread: max 32
          default 1.
  percent: max 100.
           default 95.
Example:
  ./run_sysbench.py -j pull -s ob1 -c insert -t 16
  means:
  pull -> compile -> copy -> ob1.reboot -> run sysbench insert with 16 threads -> ob1.close -> analyze trace.
"""

import sys
import subprocess
import getopt
import re

from run_test_config import *
from run_test_base import RunTestBase
from run_test_base import run_cmd_result
from run_test_base import run_cmd
from run_test_base import run_sql

def my_help():
  print __doc__

FUNC_NAME_IDX = 0
TRACE_TAG_IDX = 1
TRACE_TIME_IDX = 2
SUM = 0
CNT = 1
AVG = 2
KEY_SEQ = 3
LINE_SEQ = 4
ITER_SEQ = 5

class RunSysbench(RunTestBase):

  def __init__(self, src_root, compile_root, head_job, server, case, thread, percent):
    RunTestBase.__init__(self, src_root, compile_root, head_job, server, [self.JOB_TEST, self.JOB_TRACE])
    self.case = case
    self.thread = thread
    self.percent = percent
    self.trace_file = "trace_%s_%s" % (case, thread)
    self.arg_rt_file = "avg_rt_%s_%s" % (case, thread)
    return

  def __init(self):
    ret = 0
    if not case in self.CASES:
      ret = -1
      my_help()
    return ret

  def get_cpu_num(self):
    sql = "select cpu_capacity_max - cpu_assigned_max as cpu_num from __all_virtual_server;"
    cmd = "echo \"%s\" | ./hap.py %s.sql | egrep -A 1 'cpu_num'" % (sql, self.server)
    ret, outputs = run_cmd_result(cmd)
    cpu_num = 0
    if ret == 0 and len(outputs) > 1:
      cpu_num = outputs[1].replace('\n', '')
    else:
      ret = -1
    return ret, cpu_num

  def init_db(self, cpu_num):
    ret = 0
    if ret == 0:
      ret = run_sql("create resource unit box1 max_cpu %s, memory_size 6000000000;" % (cpu_num), self.server)
    if ret == 0:
      ret = run_sql("create resource pool pool_large unit = 'box1', unit_num = 1, zone_list = ('test');", self.server)
    if ret == 0:
      ret = run_sql("create tenant tt1 replica_num = 1,primary_zone='test', resource_pool_list=('pool_large') set ob_tcp_invited_nodes='%';", self.server)
    if ret == 0:
      ret = run_sql("alter system set large_query_threshold = '900ms';", self.server)
    if ret == 0:
      ret = run_sql("alter system set enable_record_trace_log = false;", self.server)
    return ret

  def write_trace_file(self):
    pattern = self.CASES[self.case][1]
    return run_cmd("./hap.py %s.obs0.cat | egrep '%s' > %s" % (self.server, pattern, self.trace_file))

  def get_total_time(self, line):
#    pattern = "(\[\w+\]) ([\w\s]+) u=(\d+)"
#    total_time = 0
#    time_datas = re.findall(pattern, line)
#    for data in time_datas:
#      total_time += int(data[TRACE_TIME_IDX])
#    return total_time
    pattern = "total_timeu=(\d+)"
    total_time = re.findall(pattern, line)
    return int(total_time[0])

  def cmp_by_total_time(self, line1, line2):
    cmp_ret = 0
    total_time1 = self.get_total_time(line1)
    total_time2 = self.get_total_time(line2)
    if total_time1 < total_time2:
      cmp_ret = -1
    elif total_time1 > total_time2:
      cmp_ret = 1
    return cmp_ret

  def get_trace_avg(self):
    ret = 0
    pattern = "(\[\S+\]) ([\w ]+) u=(\d+)"
    trace_fd = open(self.trace_file)
    trace_lines = trace_fd.readlines()
    trace_fd.close()
    avg_dict = {}

    # sort by total_time desc.
    trace_lines.sort(cmp=self.cmp_by_total_time, reverse=True)
    total_line = len(trace_lines)
    first_line = total_line * (100 - self.percent) / 100
    
    # line seq and iter seq is used for mutli-appearance of the same trace point in the same line.
    # like trace point in all kinds of iterator.
    line_seq = 0
    # iteration top percent lines and all trace points in each line.
    for line in trace_lines[first_line:]:
      time_datas = re.findall(pattern, line)
      # key seq is used for saving original sequence of trace points.
      key_seq = 0
      for data in time_datas:
        func_name = data[FUNC_NAME_IDX]
        trace_tag = data[TRACE_TAG_IDX]
        key = "%s %s" % (func_name, trace_tag)
        value = data[TRACE_TIME_IDX]
        # if key in dict and in same line, it means the same trace point appears in the same line again.
        # we need add suffix to key, and adjust iter seq.
        if key in avg_dict and avg_dict[key][LINE_SEQ] == line_seq:
          avg_dict[key][ITER_SEQ] -= 1
          key += " (iter: %s)" % (-avg_dict[key][ITER_SEQ])
        # now we get the right key.
        if key in avg_dict:
          avg_dict[key][SUM] += int(value)
          avg_dict[key][CNT] += 1
          avg_dict[key][KEY_SEQ] = key_seq
          avg_dict[key][LINE_SEQ] = line_seq
          avg_dict[key][ITER_SEQ] = 0
        else:
          # [sum_rt, cnt, avg, key_seq, line_seq, iter_seq]
          avg_dict[key] = [int(value), 1, 0, key_seq, line_seq, 0]
        key_seq += 1
      line_seq += 1
    
    # iteration done, compute avg for sort.
    for key, value in avg_dict.items():
      value[AVG] = 1.0 * value[SUM] / value[CNT]
    
    # sorted by avg.
    avg_list = sorted(avg_dict.iteritems(), key=lambda d:d[1][AVG], reverse=True)
    print ">> sorted by avg"
    for tup in avg_list:
      key = tup[0] if tup[1][ITER_SEQ] >= 0 else tup[0] + " (iter: 0)"
      print ">> avg=%s, count=%s: %s" % (tup[1][AVG], tup[1][CNT], key)
    print ""
    
    # sorted by seq.
    seq_list = sorted(avg_dict.iteritems(), key=lambda d:d[1][KEY_SEQ], reverse=False)
    print ">> sorted by seq"
    for tup in seq_list:
      key = tup[0] if tup[1][ITER_SEQ] >= 0 else tup[0] + " (iter: 0)"
      print ">> avg=%s, count=%s: %s" % (tup[1][AVG], tup[1][CNT], key)
    print ""
    
    # debug info.
    print ">> total line: %s, top %s%% line: %s" % (total_line, percent, total_line - first_line)
#    for i in range(total_line):
#      if i < 20 or i % 100 == 0:
#        print "total_time %s: %s" % (i, self.get_total_time(trace_lines[i]))
    
    return ret

  def sysbench(self):
    ret = 0
    case = self.case
    lua_script = self.CASES[self.case][0]
    thread = self.thread
    sql_user = "root@tt1"
    sysbench_bin = "sysbench/sysbench"
    sysbench_db = "test"
    sysbench_max_time = 60
    sysbench_table_size = 10000
    other_args = "sql_user=%s sysbench_bin=%s sysbench_db=%s sysbench_max_time=%s sysbench_table_size=%s" % (sql_user, sysbench_bin, sysbench_db, sysbench_max_time, sysbench_table_size)

    if ret == 0:
      ret = run_cmd("./hap.py %s.sysbench prepare sysbench_threads=%s sysbench_lua=sysbench/%s %s" % (self.server, thread, lua_script, other_args))
    if ret == 0:
      ret = run_sql("alter system flush plan cache;", self.server)
    if ret == 0:
      ret = run_cmd("./hap.py %s.obs0.kill -50" % (self.server));
    if ret == 0:
      ret = run_cmd("./hap.py %s.sysbench run sysbench_threads=%s sysbench_lua=sysbench/%s %s" % (self.server, thread, lua_script, other_args))
    if ret == 0:
      ret = run_cmd("./hap.py %s.stop" % (self.server))
    return ret

  def test(self):
    ret = 0
    if ret == 0:
      ret, cpu_num = self.get_cpu_num()
    if ret == 0:
      ret = self.init_db(cpu_num)
    if ret == 0:
      ret = self.sysbench()
    return ret

  def trace(self):
    ret = 0
    if ret == 0:
      ret = self.write_trace_file()
    if ret == 0:
      ret = self.get_trace_avg()
    return ret

  def run(self):
    ret = 0
    if ret == 0:
      ret = self.__init()
    if ret == 0 and self.need_do_job(self.JOB_REBOOT):
      ret = self.start_server()
    if ret == 0 and self.need_do_job(self.JOB_TEST):
      ret = self.test()
    if ret == 0 and self.need_do_job(self.JOB_TRACE):
      ret = self.trace()
    return ret

  JOB_TEST = "test"
  JOB_TRACE = "trace"
  CASES = {
    "get": ["select.lua", "SELECT pad"],
    "insert": ["tcbuyer_insert.lua", "\[perf event\].*insert into   tc_biz_order"],
    "update": ["tcbuyer_update.lua", "\[perf event\].*update tc_biz_order SET"],
    "tcbuyer": ["tcbuyer_test.lua", "\[perf event\].*trace_log"],
    "tcbuyer_select": ["tcbuyer_select.lua", "\[perf event\].*SeLecT"]
  }

  case = ""
  thread = 0
  percent = 0
  trace_file = ""
  arg_rt_file = ""

if __name__ == "__main__":
  ret = 0;
  src_root = get_src_root()
  compile_root = get_compile_root()
  head_job = "compile"
  server = ""
  case = ""
  thread = 1
  percent = 90

  # get all options and args.
  opts, args = getopt.getopt(sys.argv[1:], "j:s:c:t:p:h")
  for name, value in opts:
    if name in ["-j"]:
      head_job = value
    elif name in ["-s"]:
      server = value
    elif name in ["-c"]:
      case = value
    elif name in ["-t"]:
      thread = int(value)
    elif name in ["-p"]:
      percent = int(value)
    elif name in ["-h"]:
      my_help()

  # OK, we can run.
  test = RunSysbench(src_root, compile_root, head_job, server, case, thread, percent)
  ret = test.run()
  
  sys.exit(ret)
