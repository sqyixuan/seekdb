#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import mysql.connector
from mysql.connector import errorcode
import logging
import getopt
import time

"""
usage: create a wr snapshot and calc its overhead(cpu, memory, disk)
how to use this script:
$./wr_snapshot_test.py -h "100.88.104.180" -P 31019 -u root -t 10000000 -p ""
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:436 ===================record stats before do wr snapshot===================
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:347 tenant count for wr snapshot: 3
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:372 elapse_time: 8271619
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:373 cpu_time: 1611743
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:367 wr ash row count: 15285
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:411 sstable_data_size: 221068
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:412 memstore_data_size: 0
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:430 ash_sstable_data_size: 3972
[2023-07-03 11:09:15] INFO wr_snapshot_test.py:431 ash_memstore_data_size: 0
[2023-07-03 11:09:16] INFO wr_snapshot_test.py:469 current snap id: 7
[2023-07-03 11:09:16] INFO wr_snapshot_test.py:477 wr snapshot execution successfully! snap_id: 7
[2023-07-03 11:09:16] INFO wr_snapshot_test.py:480 ===================wr snapshot overhead:===================
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:495 elapse_time: 1.343306 sec
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:496 cpu_time: 0.282857 sec
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:497 cpu_time per tenant: 0.094286 sec
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:498 wr ash row count: 2545
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:499 max_memory_consumption: 155.623787 MB
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:500 sstable_data_size: 0.000000 MB
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:501 memstore_data_size: 6.000000 MB
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:502 ash_sstable_data_size: 0.000000 MB
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:503 ash_memstore_data_size: 0.000000 MB
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:504 ash_sstable_data_size per row: 0 byte
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:505 ash_memstore_data_size per row: 0 byte
[2023-07-03 11:09:17] INFO wr_snapshot_test.py:446 ===================do major compaction===================
[2023-07-03 11:09:18] INFO wr_snapshot_test.py:454 wait for major compaction complete
[2023-07-03 11:09:52] INFO wr_snapshot_test.py:460 major compaction execution successfully
[2023-07-03 11:09:52] INFO wr_snapshot_test.py:480 ===================wr snapshot overhead:===================
[2023-07-03 11:09:53] INFO wr_snapshot_test.py:500 sstable_data_size: 0.019508 MB
[2023-07-03 11:09:53] INFO wr_snapshot_test.py:501 memstore_data_size: 0.000000 MB
[2023-07-03 11:09:53] INFO wr_snapshot_test.py:502 ash_sstable_data_size: 0.000311 MB
[2023-07-03 11:09:53] INFO wr_snapshot_test.py:503 ash_memstore_data_size: 0.000000 MB
[2023-07-03 11:09:53] INFO wr_snapshot_test.py:504 ash_sstable_data_size per row: 0 byte
[2023-07-03 11:09:53] INFO wr_snapshot_test.py:505 ash_memstore_data_size per row: 0 byte

"""

class MyError(Exception):
  def __init__(self, value):
    self.value = value
  def __str__(self):
    return repr(self.value)

class Cursor:
  __cursor = None
  def __init__(self, cursor):
    self.__cursor = cursor
  def exec_sql(self, sql, print_when_succ = True):
    try:
      self.__cursor.execute(sql)
      rowcount = self.__cursor.rowcount
      if True == print_when_succ:
        logging.debug('succeed to execute sql: %s, rowcount = %d', sql, rowcount)
      return rowcount
    except mysql.connector.Error, e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise e
    except Exception as e:
      logging.exception("normal error, fail to execute sql: %s", sql)
      raise e
  def exec_query(self, sql, print_when_succ = True):
    try:
      self.__cursor.execute(sql)
      results = self.__cursor.fetchall()
      rowcount = self.__cursor.rowcount
      if True == print_when_succ:
        logging.debug('succeed to execute query: %s, rowcount = %d', sql, rowcount)
      return (self.__cursor.description, results)
    except mysql.connector.Error, e:
      logging.exception('mysql connector error, fail to execute sql: %s', sql)
      raise e
    except Exception as e:
      logging.exception('normal error, fail to execute sql: %s, %s', sql, str(e))
      raise e

def set_parameter(cur, parameter, value):
  sql = """alter system set {0} = '{1}'""".format(parameter, value)
  logging.info(sql)
  cur.execute(sql)
  wait_parameter_sync(cur, parameter, value)

def wait_parameter_sync(cur, key, value):
  sql = """select count(*) as cnt from oceanbase.__all_virtual_sys_parameter_stat
           where name = '{0}' and value != '{1}'""".format(key, value)
  times = 10
  while times > 0:
    logging.info(sql)
    cur.execute(sql)
    result = cur.fetchall()
    if len(result) != 1 or len(result[0]) != 1:
      logging.exception('result cnt not match')
      raise e
    elif result[0][0] == 0:
      logging.info("""{0} is sync, value is {1}""".format(key, value))
      break
    else:
      logging.info("""{0} is not sync, value should be {1}""".format(key, value))

    times -= 1
    if times == 0:
      logging.exception("""check {0}:{1} sync timeout""".format(key, value))
      raise e
    time.sleep(5)

help_str = \
"""
Help:
""" +\
sys.argv[0] + """ [OPTIONS]""" +\
'\n\n' +\
'-I, --help          Display this help and exit.\n' +\
'-V, --version       Output version information and exit.\n' +\
'-h, --host=name     Connect to host.\n' +\
'-P, --port=name     Port number to use for connection.\n' +\
'-u, --user=name     User for login.\n' +\
'-t, --timeout=name  Cmd/Query/Inspection execute timeout(s).\n' +\
'-p, --password=name Password to use when connecting to server. If password is\n' +\
'                    not given it\'s empty string "".\n' +\
'-m, --module=name   Modules to run. Modules should be a string combined by some of\n' +\
'                    the following strings: ddl, normal_dml, each_tenant_dml,\n' +\
'                    system_variable_dml, special_action, all. "all" represents\n' +\
'                    that all modules should be run. They are splitted by ",".\n' +\
'                    For example: -m all, or --module=ddl,normal_dml,special_action\n' +\
'-l, --log-file=name Log file path. If log file path is not given it\'s ' + os.path.splitext(sys.argv[0])[0] + '.log\n' +\
'\n\n' +\
'Maybe you want to run cmd like that:\n' +\
sys.argv[0] + ' -h 127.0.0.1 -P 3306 -u admin -p admin\n'

version_str = """version 1.0.0"""

class Option:
  __g_short_name_set = set([])
  __g_long_name_set = set([])
  __short_name = None
  __long_name = None
  __is_with_param = None
  __is_local_opt = None
  __has_value = None
  __value = None
  def __init__(self, short_name, long_name, is_with_param, is_local_opt, default_value = None):
    if short_name in Option.__g_short_name_set:
      raise MyError('duplicate option short name: {0}'.format(short_name))
    elif long_name in Option.__g_long_name_set:
      raise MyError('duplicate option long name: {0}'.format(long_name))
    Option.__g_short_name_set.add(short_name)
    Option.__g_long_name_set.add(long_name)
    self.__short_name = short_name
    self.__long_name = long_name
    self.__is_with_param = is_with_param
    self.__is_local_opt = is_local_opt
    self.__has_value = False
    if None != default_value:
      self.set_value(default_value)
  def is_with_param(self):
    return self.__is_with_param
  def get_short_name(self):
    return self.__short_name
  def get_long_name(self):
    return self.__long_name
  def has_value(self):
    return self.__has_value
  def get_value(self):
    return self.__value
  def set_value(self, value):
    self.__value = value
    self.__has_value = True
  def is_local_opt(self):
    return self.__is_local_opt
  def is_valid(self):
    return None != self.__short_name and None != self.__long_name and True == self.__has_value and None != self.__value

g_opts =\
[\
Option('I', 'help', False, True),\
Option('V', 'version', False, True),\
Option('h', 'host', True, False),\
Option('P', 'port', True, False),\
Option('u', 'user', True, False),\
Option('t', 'timeout', True, False, 0),\
Option('p', 'password', True, False, ''),\
# Which module to run, default is all
Option('m', 'module', True, False, 'all'),\
# Log file path, the default value will be changed to different values in the main function of different scripts
Option('l', 'log-file', True, False)
]\

def change_opt_defult_value(opt_long_name, opt_default_val):
  global g_opts
  for i in range(0, len(g_opts)):
    if g_opts[i].get_long_name() == opt_long_name:
      g_opts[i].set_value(opt_default_val)
      return
  logging.warn("failed to iter over ", opt_long_name)

def has_no_local_opts():
  global g_opts
  no_local_opts = True
  for opt in g_opts:
    if opt.is_local_opt() and opt.has_value():
      no_local_opts = False
  return no_local_opts

def check_db_client_opts():
  global g_opts
  for opt in g_opts:
    if not opt.is_local_opt() and not opt.has_value():
      raise MyError('option "-{0}" has not been specified, maybe you should run "{1} --help" for help'\
          .format(opt.get_short_name(), sys.argv[0]))

def parse_option(opt_name, opt_val):
  global g_opts
  for opt in g_opts:
    if opt_name in (('-' + opt.get_short_name()), ('--' + opt.get_long_name())):
      opt.set_value(opt_val)

def parse_options(argv):
  global g_opts
  short_opt_str = ''
  long_opt_list = []
  for opt in g_opts:
    if opt.is_with_param():
      short_opt_str += opt.get_short_name() + ':'
    else:
      short_opt_str += opt.get_short_name()
  for opt in g_opts:
    if opt.is_with_param():
      long_opt_list.append(opt.get_long_name() + '=')
    else:
      long_opt_list.append(opt.get_long_name())
  (opts, args) = getopt.getopt(argv, short_opt_str, long_opt_list)
  for (opt_name, opt_val) in opts:
    parse_option(opt_name, opt_val)

def deal_with_local_opt(opt):
  if 'help' == opt.get_long_name():
    global help_str
    print help_str
  elif 'version' == opt.get_long_name():
    global version_str
    print version_str

def deal_with_local_opts():
  global g_opts
  if has_no_local_opts():
    raise MyError('no local options, can not deal with local options')
  else:
    for opt in g_opts:
      if opt.is_local_opt() and opt.has_value():
        deal_with_local_opt(opt)
        # Only process one
        return

def get_opt_host():
  global g_opts
  for opt in g_opts:
    if 'host' == opt.get_long_name():
      return opt.get_value()

def get_opt_port():
  global g_opts
  for opt in g_opts:
    if 'port' == opt.get_long_name():
      return opt.get_value()

def get_opt_user():
  global g_opts
  for opt in g_opts:
    if 'user' == opt.get_long_name():
      return opt.get_value()

def get_opt_password():
  global g_opts
  for opt in g_opts:
    if 'password' == opt.get_long_name():
      return opt.get_value()

def get_opt_timeout():
  global g_opts
  for opt in g_opts:
    if 'timeout' == opt.get_long_name():
      return opt.get_value()

def get_opt_module():
  global g_opts
  for opt in g_opts:
    if 'module' == opt.get_long_name():
      return opt.get_value()

def get_opt_log_file():
  global g_opts
  for opt in g_opts:
    if 'log-file' == opt.get_long_name():
      return opt.get_value()

def config_logging_module(log_filenamme):
  logging.basicConfig(level=logging.DEBUG,\
      format='[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s',\
      datefmt='%Y-%m-%d %H:%M:%S',\
      filename=log_filenamme,\
      filemode='w')
  # Define log print format
  formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(filename)s:%(lineno)d %(message)s', '%Y-%m-%d %H:%M:%S')
  #######################################
  # Define a Handler to print INFO and above level logs to sys.stdout
  stdout_handler = logging.StreamHandler(sys.stdout)
  # Set log print format
  stdout_handler.setFormatter(formatter)
  # Add the defined stdout_handler log handler to the root logger
  logging.getLogger('').addHandler(stdout_handler)
  stdout_handler.setLevel(logging.INFO)

def set_query_timeout(query_cur, timeout):
  if timeout != 0:
    sql = """set @@session.ob_query_timeout = {0}""".format(timeout * 1000 * 1000)
    query_cur.exec_sql(sql)

def create_mysql_cursor(my_host, my_port, my_user, my_passwd, timeout):
  try:
    conn = mysql.connector.connect(user = my_user,
                                   password = my_passwd,
                                   host = my_host,
                                   port = my_port,
                                   database = 'oceanbase',
                                   raise_on_warnings = True)
    conn.autocommit = True
    cur = conn.cursor(buffered=True)
  except mysql.connector.Error, e:
    logging.exception('connection error')
    raise e
  query_cur = Cursor(cur)
  set_query_timeout(query_cur, timeout)
  return conn, query_cur

class WrSnapshotTest:
  wr_status_dict = {}
  conn = None
  query_cur = None
  begin_time = 0
  def __init__(self, my_host, my_port, my_user, my_passwd, timeout):
    self.conn, self.query_cur = create_mysql_cursor(my_host, my_port, my_user, my_passwd, timeout)
    self.begin_time = int(time.time() * 1000000)

  def get_tenant_num(self):
    sql = """select count(*) from __all_tenant where tenant_name not like "%META%\""""
    (desc, results) = self.query_cur.exec_query(sql)
    logging.info('tenant count for wr snapshot: %d', results[0][0])
    self.wr_status_dict["tenant_num"] = results[0][0]

  def get_wr_cpu_status(self):
    sql = """select sum(value) from gv$sysstat where class=8192 and name like "%elapse time%\""""
    (desc, results) = self.query_cur.exec_query(sql)
    elapse_time = results[0][0]
    sql = """select sum(value) from gv$sysstat where class=8192 and name like "%cpu time%\""""
    (desc, results) = self.query_cur.exec_query(sql)
    cpu_time = results[0][0]
    return (elapse_time, cpu_time)

  def get_wr_ash_row_count(self):
    sql = """select sum(value) from gv$sysstat where class=8192 and name like "%active session history%\""""
    (desc, results) = self.query_cur.exec_query(sql)
    ash_row_count = results[0][0]
    return ash_row_count
  
  def record_ash_row_count(self):
    ash_row_count = self.get_wr_ash_row_count()
    logging.info("wr ash row count: %d", ash_row_count)
    self.wr_status_dict["ash_row_count"] = ash_row_count

  def record_wr_cpu_status(self):
    elapse_time, cpu_time = self.get_wr_cpu_status()
    logging.info("elapse_time: %d", elapse_time)
    logging.info("cpu_time: %d", cpu_time)
    self.wr_status_dict["elapse_time"] = elapse_time
    self.wr_status_dict["cpu_time"] = cpu_time

  def get_wr_memory_consumption(self):
    sql = """select max(REQUEST_MEMORY_USED) from gv$ob_sql_audit where query_sql like "%__wr%\" and request_time >= {0}""".format(self.begin_time)
    (desc, results) = self.query_cur.exec_query(sql)
    if results[0][0] is None:
      logging.warn('no wr inner sql records in sql audit')
    else:
      max_memory_consumption = results[0][0]
    sql = """select max(REQUEST_MEMORY_USED) from gv$ob_sql_audit where query_sql like "%WORKLOAD_REPOSITORY%\" and request_time >= {0}""".format(self.begin_time)
    (desc, results) = self.query_cur.exec_query(sql)
    if results[0][0] is None:
      logging.warn('no wr inner sql records in sql audit')
    else:
      max_memory_consumption += results[0][0]
    return max_memory_consumption
  
  def record_wr_memory_consumption(self):
    max_memory_consumption = self.get_wr_memory_consumption()
    logging.info("max_memory_consumption: %d", max_memory_consumption)
    self.wr_status_dict["max_memory_consumption"] = max_memory_consumption

  def get_wr_table_data_size(self):
    sql = """select sum(b.data_size) from __all_virtual_table a inner join __all_virtual_tablet_meta_table b on a.tenant_id=b.tenant_id and a.tablet_id=b.tablet_id left join __all_virtual_ls_meta_table c on b.tenant_id=c.tenant_id and b.ls_id = c.ls_id and b.svr_ip=c.svr_ip and b.svr_port = c.svr_port where a.table_name like "__wr%" and a.tablet_id != 0"""
    (desc, results) = self.query_cur.exec_query(sql)
    sstable_data_size = results[0][0]
    sql = """select sum(mem_used), sum(hash_mem_used), sum(btree_mem_used) from __all_virtual_table a inner join __all_virtual_memstore_info b on a.tenant_id=b.tenant_id and a.tablet_id=b.tablet_id left join __all_virtual_ls_meta_table c on b.tenant_id=c.tenant_id and b.ls_id = c.ls_id and b.svr_ip=c.svr_ip and b.svr_port = c.svr_port where a.table_name like "__wr%" and a.tablet_id != 0"""
    (desc, results) = self.query_cur.exec_query(sql)
    memstore_data_size = results[0][0]
    if memstore_data_size is None:
      # maybe just major compactioned.
      memstore_data_size = 0
    return (sstable_data_size, memstore_data_size)
  
  def record_wr_table_data_size(self):
    sstable_data_size, memstore_data_size = self.get_wr_table_data_size()
    logging.info("sstable_data_size: %d", sstable_data_size)
    logging.info("memstore_data_size: %d", memstore_data_size)
    self.wr_status_dict["sstable_data_size"] = sstable_data_size
    self.wr_status_dict["memstore_data_size"] = memstore_data_size

  def get_wr_ash_table_data_size(self):
    sql = """select sum(b.data_size) from __all_virtual_table a inner join __all_virtual_tablet_meta_table b on a.tenant_id=b.tenant_id and a.tablet_id=b.tablet_id left join __all_virtual_ls_meta_table c on b.tenant_id=c.tenant_id and b.ls_id = c.ls_id and b.svr_ip=c.svr_ip and b.svr_port = c.svr_port where a.table_name like "__wr_active_session_history%" and a.tablet_id != 0"""
    (desc, results) = self.query_cur.exec_query(sql)
    sstable_data_size = results[0][0]
    sql = """select sum(mem_used), sum(hash_mem_used), sum(btree_mem_used) from __all_virtual_table a inner join __all_virtual_memstore_info b on a.tenant_id=b.tenant_id and a.tablet_id=b.tablet_id left join __all_virtual_ls_meta_table c on b.tenant_id=c.tenant_id and b.ls_id = c.ls_id and b.svr_ip=c.svr_ip and b.svr_port = c.svr_port where a.table_name like "__wr_active_session_history%" and a.tablet_id != 0"""
    (desc, results) = self.query_cur.exec_query(sql)
    memstore_data_size = results[0][0]
    if memstore_data_size is None:
      # maybe just major compactioned.
      memstore_data_size = 0
    return (sstable_data_size, memstore_data_size)

  def record_wr_ash_table_data_size(self):
    sstable_data_size, memstore_data_size = self.get_wr_ash_table_data_size()
    logging.info("ash_sstable_data_size: %d", sstable_data_size)
    logging.info("ash_memstore_data_size: %d", memstore_data_size)
    self.wr_status_dict["ash_sstable_data_size"] = sstable_data_size
    self.wr_status_dict["ash_memstore_data_size"] = memstore_data_size

  def record_stats(self):
    logging.info("===================record stats before do wr snapshot===================")
    self.get_tenant_num()
    self.record_wr_cpu_status()
    self.record_ash_row_count()
    # no need to record previous wr memory consumption
    # self.record_wr_memory_consumption()
    self.record_wr_table_data_size()
    self.record_wr_ash_table_data_size()

  def do_major_compaction(self):
    logging.info("===================do major compaction===================")
    sql = """ALTER SYSTEM MAJOR FREEZE TENANT=ALL"""
    self.query_cur.exec_sql(sql)
    time.sleep(1)

  def wait_till_major_compaction_finish(self):
    sql = """SELECT sum(MERGE_STATUS) FROM oceanbase.__ALL_VIRTUAL_ZONE_MERGE_INFO"""
    major_compaction_unfinished_cnt = 1
    logging.info("wait for major compaction complete")
    while major_compaction_unfinished_cnt != 0:
      (desc, results) = self.query_cur.exec_query(sql)
      major_compaction_unfinished_cnt = results[0][0]
      if major_compaction_unfinished_cnt is not 0:
        time.sleep(1)
    logging.info("major compaction execution successfully")


  def do_wr_snapshot(self):
    sql = "call dbms_workload_repository.create_snapshot()"
    self.query_cur.exec_sql(sql)
    sql = "select snap_id from CDB_WR_SNAPSHOT order by snap_id desc limit 1"
    (desc, results) = self.query_cur.exec_query(sql)
    last_snap_id = results[0][0]
    logging.info("current snap id: %d", last_snap_id)
    sql = """select count(*) from __ALL_VIRTUAL_WR_SNAPSHOT where snap_id = {0} and status != 0""".format(last_snap_id)
    (desc, results) = self.query_cur.exec_query(sql)
    if results[0][0] != 0 :
      err_msg = "wr snapshot has failure! " + str(last_snap_id)
      logging.warn(err_msg)
      raise MyError(err_msg)
    else:
      logging.info("wr snapshot execution successfully! snap_id: %d", last_snap_id)

  def calc_snapshot_overhead(self, after_compaction = False):
    logging.info("===================wr snapshot overhead:===================")
    elapse_time, cpu_time = self.get_wr_cpu_status()
    max_memory_consumption = self.get_wr_memory_consumption()
    sstable_data_size, memstore_data_size = self.get_wr_table_data_size()
    ash_sstable_data_size, ash_memstore_data_size = self.get_wr_ash_table_data_size()
    ash_row_count = self.get_wr_ash_row_count()
    elapse_time = elapse_time - self.wr_status_dict["elapse_time"]
    cpu_time = cpu_time - self.wr_status_dict["cpu_time"]
    ash_row_count = ash_row_count - self.wr_status_dict["ash_row_count"]
    # max_memory_consumption = max(max_memory_consumption, self.wr_status_dict["max_memory_consumption"])
    sstable_data_size = sstable_data_size - self.wr_status_dict["sstable_data_size"]
    memstore_data_size = memstore_data_size - self.wr_status_dict["memstore_data_size"]
    ash_sstable_data_size = ash_sstable_data_size - self.wr_status_dict["ash_sstable_data_size"]
    ash_memstore_data_size = ash_memstore_data_size - self.wr_status_dict["ash_memstore_data_size"]
    if not after_compaction:
      logging.info("elapse_time: %f sec", float(elapse_time) / 1000 / 1000)
      logging.info("cpu_time: %f sec", float(cpu_time) / 1000 / 1000)
      logging.info("cpu_time per tenant: %f sec", float(cpu_time) / 1000 / 1000 / self.wr_status_dict["tenant_num"])
      logging.info("wr ash row count: %d", ash_row_count)
      logging.info("max_memory_consumption: %f MB", float(max_memory_consumption) / 1024 / 1024)
    logging.info("sstable_data_size: %f MB", float(sstable_data_size) / 1024 / 1024)
    logging.info("memstore_data_size: %f MB", float(memstore_data_size) / 1024 / 1024)
    logging.info("ash_sstable_data_size: %f MB", float(ash_sstable_data_size) / 1024 / 1024)
    logging.info("ash_memstore_data_size: %f MB", float(ash_memstore_data_size) / 1024 / 1024)
    logging.info("ash_sstable_data_size per row: %d byte", ash_sstable_data_size / ash_row_count)
    logging.info("ash_memstore_data_size per row: %d byte", ash_memstore_data_size / ash_row_count)

  def run(self):
    self.record_stats()
    self.do_wr_snapshot()
    self.calc_snapshot_overhead()
    self.do_major_compaction()
    self.wait_till_major_compaction_finish()
    self.calc_snapshot_overhead(True)

if __name__ == '__main__':
  parse_options(sys.argv[1:])
  change_opt_defult_value('log-file', os.path.splitext(sys.argv[0])[0] + '.log')
  if not has_no_local_opts():
    deal_with_local_opts()
  else:
    check_db_client_opts()
    # log_filename = get_opt_log_file()
    config_logging_module(os.path.splitext(sys.argv[0])[0] + '.log')
    try:
      host = get_opt_host()
      port = int(get_opt_port())
      user = get_opt_user()
      password = get_opt_password()
      timeout = int(get_opt_timeout())
      logging.info('parameters from cmd: host=\"%s\", port=%s, user=\"%s\", password=\"%s\", timeout=\"%s\", log-file=\"%s\"',\
          host, port, user, password, timeout, os.path.splitext(sys.argv[0])[0] + '.log')
      test = WrSnapshotTest(host, port, user, password, timeout)
      test.run()
    except mysql.connector.Error, e:
      logging.exception('mysql connctor error', e)
      raise e
    except Exception, e:
      logging.exception('normal error %s', e)
      raise e
