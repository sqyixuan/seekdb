#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import sys

reload(sys)
sys.setdefaultencoding('utf8')

notice = "===================================================================================================\n \
**                                                                                              **\n \
**          INSTRUCTION: Paragraphs are separated by blank lines, every paragraph consists      **\n \
**          of two lines. The first line of each paragraph is where the error code may be       **\n \
**          overwritten, the second line is an if statement, and the block of statements        **\n \
**          that begins with it may have statements that override the error code.               **\n \
**                                                                                              **\n \
==================================================================================================\n \n"
args = sys.argv

def get_white_lists(white_file):
    if white_file is not None:
        if os.path.isfile(white_file):
            try:
               with open(white_file, 'r') as f:
                    file_keywords = set([line.strip() for line in f if len(line.strip()) >= 2])
                    return file_keywords
            except:
                import traceback
                traceback.print_exc()
                print("invaild whitelist")
                exit(1)
    return {}

file_keywords = get_white_lists(args[4])

def white_file(filename):
    for name in file_keywords:
         if name in filename:
             return True
         return False

key_words = {"ret = OB_SUCCESS", "OB_SUCC", "OB_FAIL(ret)", "!=", "==", "IS_NOT_INIT", ">=", "<=", " > ", " < ", \
            "is_", "has_", "need_", "OB_NOT_NULL", "REACH_TENANT_TIME_INTERVAL", "FAILEDx", "empty", "OB_TMP_FAIL", "NULL_PTR"}

def contain_key_words(line):
    for word in key_words:
        if word in line:
            return True
    return False

key_words_2 = {"if", "else", "SMART_VAR", "PH", "case", "overwrite"}
def contain_key_words_2(line):
    for word in key_words_2:
        if word in line:
            return True
    return False

key_words_3 = {"break", "continue", "overwrite", "nothing", "LOG_INFO"}
def contain_key_words_3(line):
    for word in key_words_3:
        if word in line:
            return True
    return False

def contain_fail(lines, index):
    end = False
    for i in range(index, index + 6):
        if end == False and i < len(lines):
            if is_ret_fail(lines[i]):
                return True
            if "{" in lines[i]:
                end = True
    return False

def is_ret_success(line):
    if "ret = OB_SUCCESS" in line or "OB_SUCC" in line or "INIT_SUCC" in line or "ARRAY_FOREACH" in line or "MTL_SWITCH" in line or "HEAP_VAR" in line:
        return True
    return False

def is_ret_fail(line):
    if "EventTable" not in line and "OB_TMP_FAIl" not in line and ((" ret = " in line and "OB_SUCCESS" not in line and "= OB_" in line) or "OB_FAIL" in line):
        return True
    return False

def is_return_line(line):
    if "return" in line or "break" in line or "case" in line:
        return True
    return False

def count_space_ahead(line):
    return len(line) - len(line.lstrip())

def process_file(f, res_f):
    test_result = True
    file_data = []
    for line in f:
        file_data.append(line)
    curr_line = ""
    curr_index = 0
    curr_space_count = 0
    is_fail = False
    is_if_state = False
    is_comments = False
    data = []
    for i in range(len(file_data)):
        line = file_data[i]
        if "/*" in line:
            is_comments = True
        if "*/" in line:
            is_comments = False
        if is_comments or line.strip().startswith("//") or line.strip().endswith("\\"):
            continue
        if is_fail and is_if_state and re.findall("if\s*\(.*\(", line) and "else" not in line and contain_fail(file_data, i):
            if i + 2 < len(file_data) and i - 2 > 0 and not contain_key_words(line) and curr_space_count >= count_space_ahead(line) and not contain_key_words_2(file_data[i-1]) and not contain_key_words_2(file_data[i-3]) \
                    and not contain_key_words_2(file_data[i-2]) and not contain_key_words_3(file_data[i+1]) and not contain_key_words_3(file_data[i+2]):
                res_f.write(f.name.replace('//', '/') + ":" + str(curr_index + 1) + curr_line)
                res_f.write(f.name.replace('//', '/') + ":" + str(i + 1) + line)
                res_f.write("\n")
                test_result = False
        if "if" in line and "else" not in line:
            is_if_state = True
            is_fail = False
            curr_space_count = count_space_ahead(line)
        if is_if_state and is_ret_fail(line):
            curr_line = line
            curr_index = i
            is_fail = True
        if is_ret_success(line) or is_return_line(line):
            is_if_state = False
            is_fail = False
        if "else" in line and count_space_ahead(line) < curr_space_count:
            is_fail = False
            is_if_state = False
    return test_result

if __name__=="__main__":
    src_root = args[1]  # Directory, where ob code is located
    scan_files = args[2] # Which files need to be scanned
    result_file = args[3] # Output
    file_list = open(scan_files, 'r')
    res_file = open(result_file, "w")
    test_result = True
    for file_name in file_list:
        if ("mittest/" in file_name) or white_file(file_name):
            continue
        file_name = file_name.strip()
        if not os.path.isfile(src_root + "/" + file_name.lstrip(".")):
            continue
        with open(src_root + "/" + file_name.lstrip("."), 'r') as f:
            if process_file(f, res_file) == False:
                test_result = False
    if os.path.getsize(result_file) != 0:
        res_file.close()
        res_file = open(result_file, "r+")
        old = res_file.read()
        res_file.seek(0)
        res_file.write(notice)
        res_file.write(old)

    if test_result:
        print("测试通过")
        sys.exit(0)
    else:
        print("测试不通过")
        sys.exit(1)
