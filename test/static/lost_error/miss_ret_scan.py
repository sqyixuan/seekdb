#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
import sys
import os

reload(sys)
sys.setdefaultencoding('utf8')

notice = "===================================================================================================\n \
**                                                                                              **\n \
**          INSTRUCTION: Paragraphs are separated by blank lines, the first line of each        **\n \
**          paragraph is where the error code may be lost, the rest of each paragraph is        **\n \
**          used help determine whether an error code is missing or not.                        **\n \
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
# else if statement should not contain the following keywords
key_words = {"MDS_FAIL", "OB_FAIL", "CLICK_FAIL", "OB_UNLIKELY", "OB_SUCC", " == ", "!", " > ", " < ", ">=" , "<=", "FALSE_IT", "contain", "has_", \
        "need_", "empty", "is_", "IS_", "OB_NOT_NULL", "OB_TMP_FAIL", "CAST_FAIL", "IsEnabled", "IS_EXPR_OP", "FAILEDx", "OB_BREAK_FAIL", \
        "REACH_TIME_INTERVAL", "REACH_TENANT_TIME_INTERVAL", "EXECUTE_COUNT_PER_SEC",  "NULL_PTR", "OB_LIKELY", "isupper", "islower", \
        "COMPARE_OUT", "palf_reach_time_interval", "CLICK_TMP_FAIL", "REACH_COUNT_INTERVAL", "reach_", "CUSTOM_FAIL", "OBARROW_FAIL"}

file_keywords = get_white_lists(args[4])
key_words = key_words.union(file_keywords)

def contain_key_words(line):
    for word in key_words:
        if word in line:
            return True
    return False
# else if statement below this line should not contain these keywords
key_words_2 = {"ret =", "OZ", "OY", "OX", "nothing", "=", "break", "++", "return", "by design", "_RET", "bypass", \
        "skip", "ignore", "by pass", "INFO", "TRACE", "DEBUG"}
def contain_key_words_2(line):
    for word in key_words_2:
        if word in line:
            return True
    return False

if __name__=="__main__":
    src_root = args[1]
    scan_files = args[2]
    result_file = args[3]
    part = ""   # Specify module, optional, sql
    f = open(scan_files, 'r')
    res_f = open(result_file, 'w')
    data = []
    for line in f:
        # if line.strip() != "" and line.strip() != "--" and not line.startswith("./src/" + part):
        #     continue
        if line.strip().endswith("\\"):
            continue
        splits = re.split("\s+", line)
        if len(splits) > 1 and (splits[0].endswith("//") or splits[1].startswith("//")) and "ignore" not in line and  "by design" not in line and "nothing" not in line and "bypass" not in line and "by pass" not in line:
            continue
        # data.append(line[splits[0].rfind('/')+1:]) # Only print the filename, not the path
        data.append(line)
    
    test_result = True
    for i in range(len(data)):
        line = data[i]
        if re.findall("if \(", line): 
            if "{" not in line:
                while "{" not in data[i] and i < len(data) - 1:
                    i = i + 1
                    line += data[i]
            if i+3 < len(data) and re.findall("if \(.*\(", line) and "LOG" in data[i+1] and not contain_key_words_2(data[i+1]) and "ret =" not in data[i+2] and "ret =" not in data[i+3] and "break" not in data[i+2] and not contain_key_words(line):
                res_f.write(line)
                res_f.write(data[i+1])
                res_f.write(data[i+2])
                res_f.write(data[i+3])
                res_f.write(data[i+4])
                res_f.write("\n")
                test_result = False
                
    if os.path.getsize(result_file) != 0:
        res_f.close()
        res_f = open(result_file, "r+")
        old = res_f.read()
        res_f.seek(0)
        res_f.write(notice)
        res_f.write(old)


    if test_result:
        print("测试通过")
        sys.exit(0)
    else:
        print("测试不通过")
        sys.exit(1)
    pass

