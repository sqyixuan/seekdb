#!/bin/env python2
# -*- coding: UTF-8

import sys
import re
import os
import argparse
import re

reload(sys)
sys.setdefaultencoding('utf-8')

parser = argparse.ArgumentParser(description='测试argParse模块')
parser.add_argument('-a', '--all', help='开启全量扫描模式，不带此项默认为增量扫描', action='store_true')
parser.add_argument('-d', '--debug', help='开启debug模式，不带此项默认不开启，开启后可打印运行日志', action='store_true')
parser.add_argument('-dp', '--diffPath', help='gitDiff文件路径，增量模式下可自动识别gitDiff中发生变更的文件并进行扫描（增量模式下必填）')
parser.add_argument('-p', '--path', help='项目工程路径（必填）', required=True)
parser.add_argument('-t', '--thread', help='多线程运行线程数', type=int, default='1')
args = parser.parse_args()
# Whether to enable debug mode
debug_mode = args.debug
# Whether to enable full scan mode
scan_all = args.all
# project root path
total_path = args.path
# Maximum number of threads in thread pool
thread_num = args.thread

license_text = ["Copyright (c) 2025 OceanBase.",
         "",
         "Licensed under the Apache License, Version 2.0 (the \"License\");",
         "you may not use this file except in compliance with the License.",
         "You may obtain a copy of the License at",
         "",
         "http://www.apache.org/licenses/LICENSE-2.0",
         "",
         "Unless required by applicable law or agreed to in writing, software",
         "distributed under the License is distributed on an \"AS IS\" BASIS,",
         "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
         "See the License for the specific language governing permissions and",
         "limitations under the License."
         ]

c_file_tail = {'c', 'cpp', 'C', 'h', 'hpp', 'hxx', 'cc', 'cxx', 'c++'}
file_name_pattern = re.compile('diff --git a/(.*?) b/(.*?)')
# Print information
def print_debug_info(print_content):
    if debug_mode:
        print(print_content)
# Determine if it is a C-family source file based on the file extension
def is_c_file(file_path):
    file_tail = os.path.splitext(file_path)[-1][1:]
    return file_tail in c_file_tail
# Get all files under the path and filter out the C language files
def get_all_files(root_path, parent_path, scan_file_list):
    files = os.listdir(root_path + parent_path)
    for file_path in files:
        temp_path = root_path + parent_path + file_path
        if not os.path.isdir(temp_path):
            if is_c_file(temp_path):
                scan_file_list.append(parent_path + file_path)
                print_debug_info('文件：' + temp_path + ' 是C语言文件，进入待扫描列表')
            else:
                print_debug_info('文件：' + temp_path + ' 非C语言文件，不做扫描处理')
        else:
            temp_parent_path = parent_path + file_path + '/'
            get_all_files(root_path, temp_parent_path, scan_file_list)


def use_apache(lines):
    if len(lines) < len(license_text):
        return
    if "*" in lines[0]:
        if "Copyright" not in lines[0] or "OceanBase" not in lines[0]:
            return False
        for i in range(1,len(license_text)):
            lines[i] = lines[i].strip().strip("*").strip("\n")
            lines[i] = lines[i].replace(" ", "")
            license_text[i] = license_text[i].replace(" ", "")
            if license_text[i] != lines[i] and "LicensedundertheApacheLicenseVersion2.0" not in lines[1]:
                return False
    elif "//" in lines[0]:
        if "Copyright" not in lines[0] and  "OceanBase" not  in lines[0]:
            return False
        for i in range(1,len(license_text)):
            lines[i] = lines[i].strip().strip("//").strip("\n")
            lines[i] = lines[i].replace(" ", "")
            license_text[i] = license_text[i].replace(" ", "")
            if license_text[i] != lines[i] and  "LicensedundertheApacheLicenseVersion2.0" not in lines[1]:
                return False
    else:
        return False
    return True
# Scan diff file, get the list of files to be scanned
def scan_diff_file(diff_path, scan_file_list):
    with open(diff_path, 'r') as diff:
        for line in diff.readlines():
            if line.startswith("diff --git"):
                ret = file_name_pattern.search(line)
                if ret and ret.group() is not None:
                    try:
                        if is_c_file(ret.group(1)):
                            scan_file_list.append(ret.group(1))
                    except Exception as ex:
                        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                        message = template.format(type(ex).__name__, ex.args)
                        print_debug_info('正则匹配gitDiff文件发生异常: ' + message)
                else:
                    print_debug_info('未能从gitDiff中匹配到对应文件路径：' + line)
# Check if the Apache License is used
def check_file(file_path):
    f = open(file_path)
    try:
        line = f.readline()
        lines = []
        while ("Copyright" not in line or "OceanBase" not in line) and line:
            line = f.readline()
        lines.append(line)
        for i in range(20):
            line = f.readline().strip('\n')
            if line.strip('*') != "" or line.strip('//') != "" or line != "":
                lines.append(line.strip('*').strip('//'))
    finally:
        f.close()

    if lines is None or len(lines) == 0:
        print_debug_info('读取文件内容异常，默认检查通过: ' + file_path)
        return True

    if use_apache(lines):
        print_debug_info('  FIND [Apache 2.0]')
        return True
    else:
        print_debug_info("%s >> UNDEFINED LICENCE")
        return False       


def run():
    print_debug_info('扫描工具初始化...')
    test_result = True
    skip_result = True
    final_result = ("========================================================\n"
                    "                   Apache 2.0 License扫描报告                    \n"
                    "--------------------------------------------------------\n")
    # whitelist file
    skip_file = os.path.join(os.path.split(os.path.realpath(__file__))[0], "license/skip_list")
    scan_file_list = []
    print_debug_info('开始读取白名单文件:' + skip_file)
    # Read whitelist content, write to set
    skip_file_set = set()
    if skip_file:
        with open(skip_file, 'r') as f:
            out = f.read()
            if not(out == ''):
                lines = out.split('\n')
                for line in lines:
                    skip_file_set.add(line)
    # Full scan
    if scan_all:
        print_debug_info('开始全量扫描...')
        get_all_files(total_path, '', scan_file_list)
        for file_path in scan_file_list:
            if file_path in skip_file_set:
                if os.path.isfile(total_path + file_path):
                    if check_file(total_path + file_path):
                        final_result = final_result + "文件:" + file_path + "位于白名单中，但文件信息被修改! \n"
                        skip_result = False
                continue
            else:
                print_debug_info('开始检测文件' + file_path)
                if os.path.isfile(total_path + file_path):
                    if check_file(total_path + file_path):
                        if debug_mode:
                            final_result = final_result + "文件:" + file_path + "\n扫描结果: 通过\n"
                    else:
                        test_result = False
                        final_result = final_result + "文件:" + file_path + "\n扫描结果: 不通过，文件头未能检测到Apache 2.0协议信息，请补充协议内容！\n"
    # Incremental scan
    else:
        print_debug_info('开始增量扫描...')

        diff_path = args.diffPath
        scan_diff_file(diff_path, scan_file_list)
        for file_path in scan_file_list:
            if file_path in skip_file_set:
                if os.path.isfile(total_path + file_path):
                    if check_file(total_path + file_path):
                        final_result = final_result + "文件:" + file_path + "位于白名单中，但文件信息被修改! \n"
                        skip_result = False
                continue
            else:
                if os.path.isfile(total_path + file_path):
                    if check_file(total_path + file_path):
                        if debug_mode:
                            final_result = final_result + "文件:" + file_path + "\n扫描结果: 通过\n"
                    else:
                        test_result = False
                        final_result = final_result + "文件:" + file_path + "\n扫描结果: 不通过，文件头未能检测到Apache 2.0协议信息，请补充协议内容！\n"
    print_debug_info('扫描运行完成')
    
    if test_result and skip_result:
        print("扫描通过")
        sys.exit(0)
    else:
        print(final_result)
        sys.exit(1)


if __name__ == '__main__':
    run()
