#!/bin/env python
# -*- coding: utf-8 -*-
#__author__ = 'dongyun.zdy'

import argparse
import subprocess
import sys


def run_cmd(cmd):
    # print cmd
    res = ''
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
        line = p.stdout.readline()
        res += line
        if line:
            pass
            # print line.strip()
            # sys.stdout.flush()
        else:
            break
    p.wait()
    return res


class Options():
    def __init__(self):
        self.ver = None
        self.need_server = True
        self.need_proxy = True

    def parseArg(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("version", help="version to get", default=None)
        parser.add_argument("-s", "--server", help="need server(default both)", action='store_true', dest='need_server', default=False)
        parser.add_argument("-p", "--proxy", help="need proxy(default both)", action='store_true', dest='need_proxy', default=False)
        args = parser.parse_args()
        self.ver = args.version
        if args.need_server or args.need_proxy:
            self.need_server = args.need_server
            self.need_proxy = args.need_proxy

def build_fetch_cmd(name, ver):
    cmd = "scp hudson@10.244.4.23:/data/10/oceanbase_build/" + name + '*' + ver + "* ./"
    return cmd

def bin_exists_on_remote(name, ver):
    return run_cmd("ssh hudson@10.244.4.23 'cd /data/10/oceanbase_build/;ls %s*%s*'" % (name, ver)).find('No such file') == -1

def get_bin(name, ver):
    if bin_exists_on_remote(name, ver):
        print "%s bin exist, get bin" % name
        run_cmd(build_fetch_cmd(name, ver))
    else:
        print "%s bin not exist" % name


if __name__ == '__main__':
    opt = Options()
    opt.parseArg()
    if opt.need_server:
        get_bin('observer', opt.ver)
    if opt.need_proxy:
        get_bin('obproxy', opt.ver)


