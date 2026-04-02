#!/bin/env python2
# -*- coding: UTF-8

import json
import sys
import re
import json
import os

check_only = False
debug_level = 5
reload(sys)
sys.setdefaultencoding('utf-8')

base_dir = sys.argv[1]
skip_file =  os.path.join(os.path.split(os.path.realpath(__file__))[0], "skip_list")
skip_files = []
with open(skip_file, 'r') as f:
    skip_files = f.readlines()


mulan = ["/*\n",
" * Copyright (c) 2025 OceanBase\n",
" *\n",
" * Licensed under the Apache License, Version 2.0 (the \"License\");\n",
" * you may not use this file except in compliance with the License.\n",
" * You may obtain a copy of the License at\n",
" *\n",
" *     http://www.apache.org/licenses/LICENSE-2.0\n",
" *\n",
" * Unless required by applicable law or agreed to in writing, software\n",
" * distributed under the License is distributed on an "AS IS" BASIS,\n",
" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n",
" * See the License for the specific language governing permissions and\n",
" * limitations under the License.\n",
" */\n",
"\n"
]


c0 = re.compile(r'^\s*//.*')
c1 = re.compile(r'^\s*$')
c2s = re.compile(r'^\s*\/\*')
c2e = re.compile(r'.*\*\/\s*$')

def skip_comment_head(lines):
    current_idx = -1
    cflag = False
    for line in lines:
        current_idx += 1
        # assert error 
        if current_idx > 100:
            return -1
        if cflag:
            if c2e.match(line):
                cflag = False
                continue
        if cflag or c1.match(line) or c0.match(line):
            continue
        if c2s.match(line):
            if not c2e.match(line):
                cflag = True
            continue
        return current_idx
            


def add_license(file_path, relative_path):
    with open(file_path, 'rb') as f:
        lines = f.readlines()
    n = skip_comment_head(lines)
    if n < 0:
        print("error %s !!!!" % relative_path)
        return -1
    with open(file_path, 'wb') as w:
        w.writelines(mulan)
        w.writelines(lines[n:])
        print(">> handle %s" % relative_path)



def use_mulan(lines):
#    print lines[:12]
    return mulan == lines[:15]

def check_file(file_path, relative_path):
    f = open(file_path)
    lines = f.readlines(20)
    f.close()
    if use_mulan(lines):
        if debug_level < 5:
            print("  FIND [Apache License, Version 2.0]")
        return 0
    else:
        if debug_level < 5:
            print("%s >> UNDEFINED LICENCE" % relative_path)
        return 999


lex = len(base_dir)
if not base_dir[-1] == "/":
    lex = lex + 1
for dirpath, dirnames, filenames in os.walk(base_dir):
    xpath = dirpath[lex:]
    if xpath[0:4] == ".git" or xpath[0:6] == ".obdev" or xpath[0:6] == "build_" or xpath[0:3] == "3rd":
        continue
    for name in filenames:
        if (len(name) > 2 and (name[-2:] == '.c' or name[-2:] == '.h'))\
                or (len(name) > 4 and name[-4:] == '.cpp' or name[-4:] == '.ipp'):
            file_path = os.path.join(dirpath, name)
            relative_path = os.path.join(xpath, name)
            c = False
            for skip_item in skip_files:
                if file_path.endswith(skip_item[:-1]):
                    print "skip %s since in skip list" % relative_path
                    c = True
                    break
            if (c):
                continue
            ret = check_file(file_path, relative_path)
            if not check_only and ret:
                add_license(file_path, relative_path)
