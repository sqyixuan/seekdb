#!/usr/bin/env python2

import os
import sys
import re
from run_test_config import *

def get_server_memory():
  user = os.getlogin()
  server_list = get_server_list()
  server_memory = {}
  pattern = "Mem:[ ]+\d+[ ]+\d+[ ]+(\d+)"
  for server in server_list:
    cmd = "ssh -l %s %s 'free -m'" % (user, server)
    ret, result = run_cmd_result(cmd, False)
    if ret == 0:
      memory = re.findall(pattern, result[1])
      server_memory[server] = int(memory[0])
    else:
      break
  if ret == 0:
    server_memory_sorted = sorted(server_memory.iteritems(), key=lambda d:d[1], reverse=True)
    for server, memory in server_memory_sorted:
      print "%s: %sm" % (server, memory)
  return ret

if __name__ == '__main__':
  ret = get_server_memory()
  sys.exit(ret)
