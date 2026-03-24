#!/usr/bin/python
# -*- coding: utf-8 -*-
# Dependency: cx_oracle and libclntsh.so.11.1, please note that the versions of python, oracle, and their compatibility need to be checked
# cx_oracle : https://oracle.github.io/python-cx_Oracle/
# libclntsh.so.11.1 : After installation, the path needs to be added to LD_LIBRARY_PATH

import os
import cx_Oracle
import re
import mysql.connector
from pprint import pprint
from subprocess import call
### Connection configuration, crude version can be written more friendly, will be integrated into deploy later
#orac_conn = 'tingshuai/tingshuai@100.81.152.95/test1'
orac_conn = 'tingshuai/tingshuai@127.0.0.1:1521/orcl.example.com'
mysql_host = '127.0.0.2'
mysql_port = '50801'
mysql_user = 'root'
mysql_pwd = ''
###

### test file
reject_file = 'mysql_test/var/log/hierarchical_beat.reject'
orcl_file = 'mysql_test/var/log/merge_into_normal.orac'
test_file = 'mysql_test/test_suite/merge_into/t/merge_into_normal.test'


def print_result(cursor, rfile):
    is_query =  re.match(r'^select', cursor.statement, re.I)
    if is_query  :
        try:
            for result in cursor:
                rfile.write(str(result) + '\n')
        except cx_Oracle.DatabaseError as err_msg:
            rfile.write(str(err_msg).strip() + '\n')
        except mysql.connector.errors.ProgrammingError as err_msg:
            rfile.write(str(err_msg).strip() + '\n')
        except mysql.connector.errors.DatabaseError as err_msg:
            rfile.write(str(err_msg).strip() + '\n')

    rfile.flush()

def execute_sql(cursor, sql, rfile):
    is_comment = re.match(r'^#|--', sql, re.I)
    if is_comment or not sql:
        rfile.write(sql + '\n');
    else:
        try:
            rfile.write(sql + '\n')
            cursor.execute(sql)
        except cx_Oracle.DatabaseError as err_msg:
            rfile.write(str(err_msg).strip() + '\n')
        except mysql.connector.errors.ProgrammingError as err_msg:
            rfile.write(str(err_msg).strip() + '\n')
        except mysql.connector.errors.DatabaseError as err_msg:
            rfile.write(str(err_msg).strip() + '\n')
        else:
            print_result(cursor, rfile)


class BaseExecutor:
    def __init__(self, test_file):
        self.test_file_ = None
        self.reject_file_ = None
    def clear_resource(self):
        if self.cursor_:
            self.cursor_.close()
            self.cursor_ = None
        if self.conn_:
            self.conn_.close()
            self.conn_ = None
        if self.reject_file_:
            self.reject_file_.close()
            self.reject_file_ = None
    def connect_server(self):
        print "do nothing"
    def run(self):
        try:
            self.connect_server()
            file_handler = open(self.test_file_)
            for sql_line in file_handler:
                execute_sql(self.cursor_, sql_line.strip().replace(';',''), self.reject_file_)
        finally:
            print 'clear resource'


class OracleExecutor(BaseExecutor):
    def __init__(self, test_file):
        self.test_file_ = test_file
        self.reject_file_ = open(orcl_file, 'w+')
    def connect_server(self):
        self.conn_ = cx_Oracle.connect(orac_conn)
        self.cursor_ = self.conn_.cursor();

class MySQLExecutor(BaseExecutor):
    def __init__(self, test_file, host, port, user, password):
        self.test_file_ = test_file
        self.host_ = host
        self.user_ = user
        self.pwd_ = password
        self.port_ = port
        self.reject_file_ = open(reject_file, 'w+')
    def connect_server(self):
        self.conn_ = mysql.connector.connect(user=self.user_, password=self.pwd_, host=self.host_, port=self.port_, database='test')
        self.cursor_ = self.conn_.cursor();

def main():
    orac_exe = OracleExecutor(test_file)
    orac_exe.run()
    # mysql_exe = MySQLExecutor(test_file, mysql_host, mysql_port, mysql_user, mysql_pwd);
    # mysql_exe.run()
    # os.system("diff -u " + reject_file + " " + orcl_file);

if __name__ == "__main__":
    main()
