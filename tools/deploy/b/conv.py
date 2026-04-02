#!/usr/bin/env python2
'''
Usages:
 ./tools.py help
'''
import inspect
import sys
import time
import socket
import struct
import random
import re
import binascii

def mktime_filter():
    '''EU: convert time to int64_t from stdin, output to stdout'''
    print sys.stdin.readline().strip()
    while True:
        line = sys.stdin.readline()
        if not line: break
        print str2ts(line.strip())

def str2ts(str):
    '''EU: convert str to ts(seconds), default format: "2013-06-18 13:15" '''
    return time.mktime(time.strptime(str, '%Y-%m-%d %H:%M:%S'))

def ts2str(ts):
    '''EU: convert ts(seconds) to str'''
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(ts)/1e6))

def ip2str(ip):
    '''EU: convert ip(int) to str(127.0.0.1)'''
    return socket.inet_ntoa(struct.pack('I',int(ip)))

def int2binary(x):
    '''EU: convert int to binary format string'''
    return '{0:064b}'.format(int(x))

def reverse(x):
    '''EU: reverse string'''
    return ''.join(reversed(x))

def int2km(x):
    if x < 1<<10:
        return '%3dB'%(x)
    if x < 1<<20:
        return '%3dK'%(x>>10)
    if x < 1<<30:
        return '%3dM'%(x>>20)
    return '%3dG'%(x>>30)

def maps_filter():
    '''EU: cat /proc/<pid>/maps | ./conv.py maps_filter'''
    addr_pat = '[0-9A-Fa-f]+'
    for line in sys.stdin:
        m = re.match('(%s)-(%s)'%(addr_pat, addr_pat), line)
        if not m:
            print line,
        start, end = int(m.group(1),16), int(m.group(2), 16)
        print '%s %s'%(int2km(end - start), line),

def dmesg_filter():
    '''EU: dmesg | ./conv.py dmesg_filter'''
    start_time = time.time() - float(open('/proc/uptime').read().split()[0])
    def format_time(seconds):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(seconds))
    print re.sub('(?m)^\[([0-9.]+)\]', lambda m: format_time(float(m.group(1)) + start_time), sys.stdin.read())

def rand_str(len):
    '''EU: ./conv.py rand_str 16'''
    def rand_char():
        return chr(ord('a') + random.randint(1, 16))
    print ''.join(map(lambda x: rand_char(), range(int(len))))

import hashlib

def passwd(token):
    '''EU: ./conv.py passwd yourpassword'''
    salt = 'dmhqekgkdebhjjojqnnqphkilhgnglcbocndhmfhqfgcdbpodfipcqnlcqdlcpgljqdcqoqoodpnjoqgonqbnplefcnbfbodijeljjmenihgfbeihcgfcgfmhcpjogeieqhqlkehleignnbmeddmoobclqpgjjkbpdhgfongedckhmnieqcimlpgohqekfnncehqgcmpogndhdppmpcljqghlbcelijhglhfpmminjbdenbfegbqnqpjjbiihmob'
    return hashlib.md5(salt + token).hexdigest()

def randstr(len):
    '''EU: ./conv.py randstr len'''
    return ''.join(chr(random.randint(ord('a'),ord('z'))) for i in range(int(len)))
def hex2bin():
    '''EU: cat data | ./conv.py hex2bin #hex-file format: 0xaa 0xbb'''
    return binascii.unhexlify(''.join(re.findall('0x(..)', sys.stdin.read())))
    
def help(msg=None):
    def format_args(k_list, v_list):
        return ', '.join(len(v_list) + i >= len(k_list) and '%s=%s'%(k, v_list[len(v_list) + i -len(k_list)]) or '%s'%(k)
                         for i, k in enumerate(k_list))
    if msg: print msg
    for k, v in globals().items():
        if not callable(v): continue
        if not v.__doc__ or not v.__doc__.startswith('EU: '): continue
        args = inspect.getargspec(v)
        print '%s(%s):\n\t%s'%(k, format_args(args.args, args.defaults or []), v.__doc__[len('EU:'):])

if __name__ == '__main__':
    len(sys.argv) >= 2  or help() or sys.exit(1)
    func = globals().get(sys.argv[1])
    callable(func) or help('%s is not callable' % (sys.argv[1])) or sys.exit(2)
    ret = func(*sys.argv[2:])
    if ret != None:
        print ret
