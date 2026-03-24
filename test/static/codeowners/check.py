#!/bin/env python2
# -*- coding: UTF-8

import json
import sys
import re
import json
import os
import ssl
from httplib import HTTPSConnection

reload(sys)
sys.setdefaultencoding('utf-8')

context = ssl.create_default_context()
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE

ownerfile = sys.argv[1]
if len(sys.argv) == 2:
    base_dir = os.path.abspath(os.path.dirname(ownerfile))
else:
    base_dir = sys.argv[2]

def get_user_list():
    try:
        conn = HTTPSConnection('work.oceanbase-dev.com', 443, timeout=6, context=context)
        conn.request('GET', '/obdev/user/get_all_domains')
        resp = conn.getresponse()
        if resp.status == 200:
            users = json.loads(resp.read())["data"]
            users = [u"@{}".format(user) for user in users]
            users.extend([u"@ob_flow_robot"])
            return users;
        else:
            return None
    except Exception,e:
        return None

def get_all_files():
    lex = len(base_dir)
    if not base_dir[-1] == "/":
        lex = lex + 1
    allfile=[]
    skipPath=""
    for dirpath, dirnames, filenames in os.walk(base_dir):
        xpath = dirpath[lex:]

        if dirpath != base_dir and '.git' in filenames:
            skipPath = dirpath
            continue

        if skipPath != "" and dirpath.startswith(skipPath):
            continue

        if 0 != len([i for i in [".git", ".obdev", "build_", ".ccls-cache", ".vscode"] if i == xpath[0:len(i)]]):
            continue
#        for dir in dirnames:
#            allfile.append(os.path.join(xpath, dir))
        for name in filenames:
            allfile.append(os.path.join(xpath, name))
    return allfile

def match(p, c, str):
    if p["type"] == 0:
        if p["rule"] == str:
            return 2
    else:
        if "creg" not in p:
            if p["type"] == 1:
                rr = '%s\\Z(?ms)' % p["rule"].replace("*","[^/]*")
            elif p["type"] == 2:
                rr = '%s\\Z(?ms)' % p["rule"].replace("**",".*")
            else:
                return 0
            p["creg"] = re.compile(rr)
        if re.match(p["creg"], str) and p["lex"] > c["lex"]:
            return 1
    return 0

def warn_log(no, line, desc):
    print "[警告] Line %d: %s\n> %s" % (no, line, desc)

def error_log(no, line, desc):
    print "[错误] Line %d: %s\n> %s" % (no, line, desc)

def summary(error, warn):
    print "===== CODEOWNERS 静态检查结果 ====="
    print "[严重的错误]"
    print "重复的规则     : %d" % len(error["duplication"])
    print "无效的规则     : %d" % len(error["invaild"])
    print "无效的所有者   : %d" % len(error["notexist"])
    print "无所有者的规则 : %d" % len(error["noowner"])
    print "无备选者的规则 : %d" % len(error["single"])
    print "未被规则保护   : %d" % len(error["no_hit_file"])
    print "[值得关注的警告]"
    print "无效空行       : %d" % len(warn["empty"])
    print "未命中的规则   : %d" % len(warn["no_hit_rule"])


def main():
    owners = {}
    users = get_user_list()
    files = get_all_files()

    if not users:
        print "get user list failed."
        sys.exit(1)

    error = {
        "duplication" : [],
        "invaild" : [],
        "notexist" : [],
        "noowner" : [],
        "single" : [],
        "no_hit_file" : []
    }

    warn = {
        "no_hit_rule": [],
        "empty": []
    }

    of = open(ownerfile)
    lines = []
    for lstr in of:
        lines.append(lstr)
        lineno = len(lines)
        lstr = lstr.strip('\n')
        line = lstr.split()
        if not line:
            warn_log(lineno, lstr, "无意义空行")
            warn["empty"].append(lineno)
            continue
        rname = line[0]
        if not line[0]:
            warn_log(lineno, lstr, "无意义空行")
            warn["empty"].append(lineno)
            continue
        rname = rname.lstrip('/')
        if rname in owners:
            ono = owners[rname]['no']
            error_log(lineno, lstr, "和行[%d] 有同样的规则 '%s'" % (ono, rname))
            error['duplication'].append(lineno)
            continue
        if rname.find('?') > -1 or rname.find('[') > -1 or rname.find(']') > -1:
            error_log(lineno, lstr, "使用了不被支持的glob语法 ?,[,]")
            error['invaild'].append(lineno)
        own = 0
        for idx in range(1,len(line)):
            item = line[idx]
            if item not in users:
                error_log(lineno, lstr, "不能识别的用户 %s" % item)
                error['notexist'].append(lineno)
            else:
                own = own + 1

        if own == 0:
            error_log(lineno, lstr, "没有有效的所有者")
            error['noowner'].append(lineno)
        else:
            type = 0
            lex = len(rname)
            if rname.find("**") > -1:
                type = 2
                lex = lex - 2.4
            elif rname.find("*") > -1:
                type = 1
                lex = lex - 1.2
        owners[rname] = { "type" : type, "lex" : lex, "hit" : 0, "no": lineno, "rule": rname, "lstr": lstr }
        if own == 1 and rname != "**":
            error_log(lineno, lstr, "没有有效的备用者")
            error['single'].append(lineno)
    of.close()

    for file in files:
        c = {"type" :2, "lex" : -0.3, "hit" :0, "no": -1}
        for k,p in owners.items():
            r = match(p, c, file)
            if r > 0:
                c = p
                if r == 2:
                    break
        if c["no"] < 0:
            error_log(-1, file, "未受规则保护的文件")
            error["no_hit_file"].append(file)
        else:
            c["hit"] = c["hit"] + 1

    for k,p in owners.items():
        if p["hit"] == 0:
            warn_log(p["no"], p["lstr"], "未保护任何文件的规则")
            warn["no_hit_rule"].append(p["no"])

    summary(error, warn)

    # enable this & `mv CODEOWNERS.new CODEOWNERS` to remove useless item.
    # with open('CODEOWNERS.new', 'w+') as f:
    #     for i in range(len(lines)):
    #         if i + 1 not in warn["no_hit_rule"]:
    #             f.write(lines[i])

    for k,v in error.items():
        if len(v) > 0:
            sys.exit(1)

if __name__ == '__main__':
    main()
