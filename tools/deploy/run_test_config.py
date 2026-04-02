#!/usr/bin/env python2

import os
import subprocess

SRC_ROOT = "~/oceanbase"
TMPFS_ROOT = "/tmpfs/%s" % (os.getlogin())
SELVER_LIST = [\
  "10.125.224.5",\
  "10.125.224.7",\
  "10.125.224.15",\
]

def get_src_root():
  return SRC_ROOT

def get_compile_root():
  if not os.path.exists(TMPFS_ROOT):
    compile_root = SRC_ROOT
  else:
    compile_root = TMPFS_ROOT
  return compile_root

def get_server_list():
  return SELVER_LIST

def run_cmd_result(cmd, verbose = True):
  if verbose:
    print "**** run cmd ****: %s" % (cmd)
  p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  ret = p.wait()
  outputs = ""
  if ret == 0:
    ret = p.returncode
    outputs = p.stdout.readlines()
  return ret, outputs

def run_cmd(cmd, verbose = True):
  if verbose:
    print "**** run cmd ****: %s" % (cmd)
  return subprocess.call(cmd, shell=True)
