#!/usr/bin/env python2.6
# ecoding=utf-8
# This file defines some structs and its operations about mysql test,
# and will be part of `deploy.py' script.
# Last change: 2013-04-26 14:17:05 #

from psmalltest import *
import os

white_dir_list=['static_engine', 'rowid']
white_file_list=['cg_test', 
                  'px.xxtest',
                  'expr.func_dump_oracle',
                  'expr_elt',
                  'update_my',
                  'funcs.datetime_func_oracle',
                 ]

def is_static_cg_test(testcase):
   return True

def test_static_cg():
   assert is_static_cg_test('t/is_not_static.test') == False
   print "1 ok"
   assert is_static_cg_test('mysql_test/t/cg_test.test') == True
   print "2 ok"
   assert is_static_cg_test('/home/longzhong.wlz/oceanbase/tools/deploy/mysql_test/t/is_not_static.test') == False
   print "3 ok"
   assert is_static_cg_test('/home/longzhong.wlz/oceanbase/tools/deploy/mysql_test/t/explain.test') == True
   print "4 farm case ok"
   assert is_static_cg_test('/home/longzhong.wlz/oceanbase/tools/deploy/mysql_test/test_suite/static_engine/t/s_static.test') == True
   print "5 ok"
   assert is_static_cg_test('/home/longzhong.wlz/oceanbase/tools/deploy/mysql_test/test_suite/static_engine2/t/cg.test') == False
   print "6 ok"
   assert is_static_cg_test('/home/longzhong.wlz/oceanbase/tools/deploy/mysql_test/test_suite/insert/t/insert_and_update_not_null_oracle.test')  == True
   print "7 ok"
   assert is_static_cg_test('/home/longzhong.wlz/oceanbase/tools/deploy/mysql_test/test_suite/px/t/xxtest.test')  == True
   print "8 white file list ok"

if __name__ == '__main__':
  test_static_cg()
