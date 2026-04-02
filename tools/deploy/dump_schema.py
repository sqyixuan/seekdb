import socket
import argparse
import sys
import os

server_host = '10.125.224.13'
server_port = 15780

def socket_send(buf):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.connect((server_host, server_port))
  sock.send(buf)
  result = sock.recv(2048)
  os.system('rm -rf dump_schema.log')
  os.system('wget {0} > /dev/null 2>&1'.format(result))
  print os.getcwd() +'/dump_schema.log'
  sock.close()

parser = argparse.ArgumentParser(description='dump schema')
parser.add_argument('-i', help='host')
parser.add_argument('-p', help='svr_port')
parser.add_argument('-n', help='tenant_name, default sys')
parser.add_argument('-v', help='schema_version, default -1(latest)')

if __name__ == '__main__':
  arg = parser.parse_known_args(sys.argv[1:])
  host, port, tenant, version = arg[0].i, arg[0].p, arg[0].n, arg[0].v
  if host is None or port is None:
    print 'invalid arguments'
    sys.exit()
  if tenant is None:
    tenant = 'sys'
  if version is None:
    version = '-1'
  send_buf = ','.join([host, port, tenant, version])
  socket_send(send_buf)
