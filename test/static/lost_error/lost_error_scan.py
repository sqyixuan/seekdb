#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re
import sys
import os
import argparse

reload(sys)
sys.setdefaultencoding('utf8')

notice = "===================================================================================================\n \
**                                                                                              **\n \
**          INSTRUCTION: Paragraphs are separated by blank lines, the first line of each        **\n \
**          paragraph is where the error code may be lost, the rest of each paragraph is        **\n \
**          used help determine whether an error code is missing or not.                        **\n \
**                                                                                              **\n \
==================================================================================================\n \n"

parser = argparse.ArgumentParser(description='测试argParse模块')
parser.add_argument('-dp', '--diff_path', help='gitDiff文件路径，增量模式下可自动识别gitDiff中发生变更的文件并进行扫描（增量模式下必填）')
parser.add_argument('-s', '--result_path', help='结果文件', required=True)
parser.add_argument('-w', '--white-file',dest="white_file", help="whitelist file")
args = parser.parse_args()


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

file_keywords = get_white_lists(args.white_file)
# else if statement should not contain the following keywords
key_words = {"MDS_FAIL", "OB_FAIL", "CLICK_FAIL", "OB_UNLIKELY", "OB_SUCC", " == ", "!", " > ", " < ", ">=" , "<=", "FALSE_IT", "contain", "has_", \
        "need_", "empty", "is_", "IS_", "OB_NOT_NULL", "OB_TMP_FAIL", "CAST_FAIL", "IsEnabled", "IS_EXPR_OP", "FAILEDx", "OB_BREAK_FAIL", \
        "REACH_TIME_INTERVAL", "REACH_TENANT_TIME_INTERVAL", "EXECUTE_COUNT_PER_SEC",  "NULL_PTR", "OB_LIKELY", "isupper", "islower", \
        "COMPARE_OUT", "palf_reach_time_interval", "CLICK_TMP_FAIL", "REACH_COUNT_INTERVAL", "reach_", "CUSTOM_FAIL"}

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
# Find all .cpp and .h files, then exclude files related to unittest and ccls-cache
def valid_file(line):
    filename = ""
    if line[0:3] == "+++":
        if (".cpp" in line or ".h" in line) and ("unittest" not in line and ".ccls-cach" not in line):
            filename = line.replace("+++ b/", "")
            filename = filename.strip()
            pass

    return filename


def get_successive_new_content(index, total, filename):
    contents = []
    while index <= total:
        line_text = read_line(index)
        if line_text[0] == "+":
            contents.append("{}: {}".format(filename.strip(), line_text[1:]))
        else:
            break

        if len(contents) >= 8:
            break
        index = index + 1

    return contents


def get_file_line():
    file = args.diff_path
    res = os.popen("sed -n '$=' {}".format(file))
    line = res.read()
    if line:
        return int(line)
    else:
        return 0


def read_line(line):
    file = args.diff_path
    res = os.popen("sed -n {}p {}".format(line, file))
    return res.read()


def get_valid_content():
    total = get_file_line()
    index = 1
    filename = ""
    data = []
    while index <= total:
        line_text = read_line(index)
        if line_text[0:3] == "+++":
            filename = valid_file(line_text)
        # Only check the content of newly added files
        contents = []
        if filename and line_text[0] == "+" and re.search(r'else if\s+\(.*\(', line_text):
            contents = get_successive_new_content(index, total, filename)
            # At least 3 lines are required
            if len(contents) >= 3:
                data.extend(contents)

        if len(contents) > 0:
            index = index + len(contents)
        else:
            index = index + 1

    return data


def run():
    res_f = open(args.result_path, 'w')
    data = []
    res_f.write(notice)
    contents = get_valid_content()
    for line in contents:
        if line.strip().endswith("\\"):
            continue
        splits = re.split("\s+", line)
        if len(splits) > 1 and (splits[0].endswith("//") or splits[1].startswith(
                "//")) and "by design" not in line and "nothing" not in line and "bypass" not in line and "by pass" not in line:
            continue
        data.append(line)

    test_result = True
    for i in range(len(data)):
        line = data[i]
        if re.findall("if \(", line):
            if "{" not in line:
                while "{" not in data[i] and i < len(data) - 1:
                    i = i + 1
                    line += data[i]
            if i + 2 < len(data) and re.findall("if \(.*\(", line) and "LOG" in data[i + 1] and not contain_key_words_2(
                    data[i + 1]) and "ret =" not in data[i + 2] and "break" not in data[
                i + 2] and not contain_key_words(line):
                res_f.write(line)
                res_f.write(data[i + 1])
                res_f.write(data[i + 2])
                res_f.write("\n")
                test_result = False

    if test_result:
        print("测试通过")
        sys.exit(0)
    else:
        print("测试不通过")
        sys.exit(1)
    pass


if __name__ == "__main__":
    run()

