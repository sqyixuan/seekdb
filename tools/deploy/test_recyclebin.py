#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import traceback
import time
import mysql.connector
import random
from mysql.connector import errorcode


def fetch_tenant_ids(cur):
  try:
    tenant_id_list = []
    cur.execute('select /*+read_consistency(WEAK) */ distinct tenant_id from oceanbase.__all_tenant')
    results = cur.fetchall()
    print 'there has %s distinct tenant ids' % len(results)
    for r in results:
      tenant_id_list.append(r[0])
    return tenant_id_list
  except mysql.connector.Error, e:
    print e
    traceback.print_exc()
    raise e
  except Exception, e:
    traceback.print_exc()
    raise e

def fetch_recycle_objects(cur, tenant_id, recycle_objs):
  try:
    expire_time = (time.time() - (60 * 60 * 24 * 7)) * 1000000;
    print 'tenant_id is %d, expire_time is %ld' % (tenant_id, expire_time)
    select_sql =  'select object_name, gmt_create, type, database_id, table_id, tablegroup_id, original_name from oceanbase.__all_recyclebin where tenant_id = {0} and type != 2 and time_to_usec(gmt_create) > {1}'.format(tenant_id, expire_time)
    print 'execute %s ...' % select_sql
    cur.execute(select_sql)
    results = cur.fetchall()
    print 'there are total %d recycle objects' % len(results)
    for r in results:
      recycle_objs.append([tenant_id, r[0], r[1], r[2], str(r[6]).decode('utf-8')])
  except mysql.connector.Error, e:
    print e
    traceback.print_exc()
    raise e
  except Exception, e:
    traceback.print_exc()
    raise e

def random_choose():
  i = random.randint(0, 10)
  if (i == 5):
    return True
  else:
    return False
   
def random_choose_recycle_objs(cur, conn, recycle_objs,f):
  for recycle_obj in recycle_objs:
    if (random_choose()):
      print "object_name = [%s]\n" % recycle_obj[1]
      print "object_name = [%s], gmt_create = [%s], type=[%d], origin_name = [%s]" % (recycle_obj[1], recycle_obj[2], recycle_obj[3], recycle_obj[4])
      expire_recycle_object(cur, conn, recycle_obj, f)

def expire_recycle_object(cur, conn, recycle_obj,f):
  try:
    print "expire recycle object, tenant_id = %d, object_name = %s" % (recycle_obj[0], recycle_obj[1])
    update_sql = "update oceanbase.__all_recyclebin set gmt_create = date_sub(gmt_create, interval 7 day) where tenant_id = {0} and object_name = '{1}'".format(recycle_obj[0], recycle_obj[1]) 
    print "execute sql %s ..." % update_sql
    obj_str = "{0}, {1}, {2}, {3}, {4}\n".format(recycle_obj[0], recycle_obj[1], recycle_obj[2], recycle_obj[3], recycle_obj[4].decode('utf-8'))
    f.write(obj_str)
    cur.execute(update_sql)
    conn.commit()
  except mysql.connector.Error, e:
    print e
    traceback.print_exc()
    raise e
  except Exception, e:
    traceback.print_exc()
    raise e

def print_recycle_objects(recycle_objs):
  for recycle_obj in recycle_objs:
    print "object_name = [%s], gmt_create = [%s], type=[%d], origin_name = [%s]" % (recycle_obj[1], recycle_obj[2], recycle_obj[3], recycle_obj[4])

def do_test(my_host, my_port, my_user, my_passwd):
  global f
  try:
    conn = mysql.connector.connect(user=my_user, password=my_passwd, host=my_host, port=my_port, database='oceanbase', charset='utf8')
    cur = conn.cursor()
    f = open("./expire.log", 'w')
    while (True):
      try:
        tenant_id_list = fetch_tenant_ids(cur)
        if len(tenant_id_list) <= 0:
          print 'distinct tenant id count is <= 0'
          raise MyError(1)
        print "fetch recycle object for each object"
        f.write('-----------------------------------\n')
        f.write(time.asctime() + '\n')
        for tenant_id in tenant_id_list:
          f.write('tenant_id = [' + bytes(tenant_id) + ']\n')
          recycle_objs = []
          fetch_recycle_objects(cur, tenant_id, recycle_objs)
          #print "====================================="
          #print_recycle_objects(recycle_objs)
          print "============= choose ================"
          random_choose_recycle_objs(cur, conn, recycle_objs,f) 
          f.write('\n')
        print "sleep 20s ..."
        time.sleep(20)
      except mysql.connector.Error, e:
        print e
        traceback.print_exc()
        raise e
      except Exception, e:
        traceback.print_exc()
        raise e
  except mysql.connector.Error, e:
    print e
    traceback.print_exc()
    raise e
  except Exception, e:
    traceback.print_exc()
    raise e
  finally:
    cur.close()
    conn.close()
    f.close()


if __name__ == "__main__":
  reload(sys)  
  sys.setdefaultencoding('utf8') 
  if 5 != len(sys.argv) :
    print "ERROR: invalid parameter count, the command should be like: \"./XXX.py host port user password\""
  elif False == sys.argv[2].isdigit():
    print "ERROR: port is not an integer, the command should be like: \"./XXX.py host port user password\""
  else:
    my_host = sys.argv[1]
    my_port = int(sys.argv[2])
    my_user = sys.argv[3]
    my_passwd = sys.argv[4]
    print "parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\"" % (my_host, my_port, my_user, my_passwd)
    do_test(my_host, my_port, my_user, my_passwd)

