#!/usr/bin/env python2.6

# -*- coding:utf-8 -*-

import sys
import os

UNIT_LEN = 5
svr_map = {}
svr_num = 0
child_map = {}
role_map = {}
rtype_map = {}
leader = ""
svr_print_cnt_map = {}
line_list = []


def recusive_print_svr(level, svr):
    cur_level = level
    cur_svr = svr
    cur_len = len(cur_svr)
    cur_print_cnt = svr_print_cnt_map[cur_svr]
    cur_print_cnt += 1
    svr_print_cnt_map[cur_svr] = cur_print_cnt
    out_str = cur_svr + "-(" + role_map[cur_svr][0] + "-" + rtype_map[cur_svr] + ")"
    if cur_print_cnt > 1:
        out_str += "---ERROR"
    print(str(out_str))

    if cur_print_cnt > 1:
        return
    cur_children = child_map.get(cur_svr)
    if cur_children:
        cur_level += 1
        cur_child_cnt = len(cur_children)
        ci = 0
        while ci < cur_child_cnt:
            print_left_padding(cur_level, False)
            print_left_padding(cur_level, True)
            recusive_print_svr(cur_level, cur_children[ci])
            ci += 1


def print_left_padding(level, with_svr):
    factor = level
    if level > 0:
        sys.stdout.write('|')
        factor -= 1
        while factor > 0:
            factor -= 1
            sys.stdout.write('   ')
            sys.stdout.write('|')
    if with_svr == True:
        sys.stdout.write('---')
    else:
        print





if __name__ == '__main__':
    if len(sys.argv) < 6 :
        print "*******************************************************************************************"
        print "Author: debin.jdb"
        print "Usages:"
        print "        ./topo-tree.py  svr_ip     svr_port  table_id          partition_idx  partition_cnt"
        print "Demo:"
        print "        ./topo-tree.py  127.0.0.1  29611     1100611139453777   0              1"
        print "*******************************************************************************************"
        sys.exit(1)
    arg0 = sys.argv[0]
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
    arg3 = sys.argv[3]
    arg4 = sys.argv[4]
    arg5 = sys.argv[5]

#    print('exec : ./query_topo_info.sh ' + arg1 + " " + arg2 + " " + arg3 + " " + arg4 + " " + arg5)
#    print "----------------------------------------------------------"
#    print "exec result:\n"
    os.system('./query_topo_info.sh ' + arg1 + " " + arg2 + " " + arg3 + " " + arg4 + " " + arg5)
#    print "----------------------------------------------------------\n"

    thefile = open("/tmp/topo-query-result.txt")
    line_list = thefile.readlines()
    line_list.pop(0)

    for line in line_list:
        #print line
        elements = line.split('\t')
        #print elements
        server = elements[0] + ":" + elements[1]
        #print server
        role_map[server] = elements[5]
        key_num = svr_map.get(server)
        if not key_num:
            svr_num += 1
            svr_map[server] = svr_num
            key_num = svr_map.get(server)
        if elements[5] == "LEADER":
            leader = server
            rtype_map[leader] = "0"
        children = elements[-1][1:]
        child_list = children.split('}')
        child_list.pop()  #del last char
        map_val = []
        for child in child_list:
            child = child[1:]  #del first char
            child_attrs = child.split(', ')
            addr = child_attrs[0].split('"')[1]
            rtype = child_attrs[3].split(':')[1]
            rtype_map[addr] = rtype
            num = svr_map.get(addr)
            if not num:
                svr_num += 1
                svr_map[addr] = svr_num
            map_val.append(addr)
        child_map[server] = map_val
#    print("---------- parent : children_list --------")
    for j in child_map:
        svr_print_cnt_map[j] = 0
#        print j, ":", child_map.get(j)
#    print "-------------------------------------------\n"

    print "++++++++++ Topo tree ++++++++++"
    print "---------- ip:port-(role-replica_type)\n"
    level = 0
    recusive_print_svr(level, leader)
    print "\n------------------------------------"
    thefile.close()


