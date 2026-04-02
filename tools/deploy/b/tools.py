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
import re

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

def int2km(x):
    if x < 1<<10:
        return '%3dB'%(x)
    if x < 1<<20:
        return '%3dK'%(x>>10)
    if x < 1<<30:
        return '%3dM'%(x>>20)
    return '%3dG'%(x>>30)

def maps_filter():
    '''EU: cat /proc/<pid>/maps | tools.py maps_filter'''
    addr_pat = '[0-9A-Fa-f]+'
    for line in sys.stdin:
        m = re.match('(%s)-(%s)'%(addr_pat, addr_pat), line)
        if not m:
            print line,
        start, end = int(m.group(1),16), int(m.group(2), 16)
        print '%s %s'%(int2km(end - start), line),

def phyop2str(idx):
    '''EU: convert phy_op_id(3) to "PHY_FILTER"'''
    phy_op_def = '''
      PHY_INVALID               = 0,
      PHY_PROJECT               = 1,
      PHY_LIMIT                 = 2,
      PHY_FILTER                = 3,
      PHY_TABLET_SCAN           = 4,
      PHY_TABLE_RPC_SCAN        = 5,
      PHY_TABLE_MEM_SCAN        = 6,
      PHY_RENAME                = 7,
      PHY_TABLE_RENAME          = 8/*deprecated*/,
      PHY_SORT                  = 9,
      PHY_MEM_SSTABLE_SCAN      = 10,
      PHY_LOCK_FILTER           = 11,
      PHY_INC_SCAN              = 12,
      PHY_UPS_MODIFY            = 13,
      PHY_INSERT_DB_SEM_FILTER  = 14,
      PHY_MULTIPLE_SCAN_MERGE   = 15,
      PHY_MULTIPLE_GET_MERGE    = 16,
      PHY_VALUES                = 17,
      PHY_EMPTY_ROW_FILTER      = 18,
      PHY_EXPR_VALUES           = 19,
      PHY_ROW_COUNT             = 20,
      PHY_WHEN_FILTER           = 21,
      PHY_CUR_TIME              = 22,
      PHY_UPS_EXECUTOR,
      PHY_TABLET_DIRECT_JOIN,
      PHY_MERGE_JOIN,
      PHY_MERGE_EXCEPT,
      PHY_MERGE_INTERSECT,
      PHY_MERGE_UNION,
      PHY_ALTER_SYS_CNF,
      PHY_ALTER_TABLE,
      PHY_CREATE_TABLE,
      PHY_DEALLOCATE,
      PHY_DROP_TABLE,
      PHY_DUAL_TABLE_SCAN,
      PHY_END_TRANS,
      PHY_PRIV_EXECUTOR,
      PHY_START_TRANS,
      PHY_VARIABLE_SET,
      PHY_TABLET_GET,
      PHY_SSTABLE_GET,
      PHY_SSTABLE_SCAN,
      PHY_UPS_MULTI_GET,
      PHY_UPS_SCAN,
      PHY_RPC_SCAN,
      PHY_DELETE,
      PHY_EXECUTE,
      PHY_EXPLAIN,
      PHY_HASH_GROUP_BY,
      PHY_MERGE_GROUP_BY,
      PHY_INSERT,
      PHY_MERGE_DISTINCT,
      PHY_PREPARE,
      PHY_SCALAR_AGGREGATE,
      PHY_UPDATE,
      PHY_TABLET_GET_FUSE,
      PHY_TABLET_SCAN_FUSE,
      PHY_ROW_ITER_ADAPTOR,
      PHY_INC_GET_ITER,
      PHY_KILL_SESSION,
      PHY_OB_CHANGE_OBI,
      PHY_ADD_PROJECT,
      PHY_UPS_MODIFY_WITH_DML_TYPE, /*62: 0.4.2 end here*/

      PHY_TABLET_SCAN_V2,
      PHY_TABLET_GET_V2,
      PHY_UNIQUE,
      PHY_INDEX_TRIGGER,
      PHY_SERVER_STAT,
      PHY_CREATE_INDEX,
      PHY_INTERVAL_SAMPLE,
      PHY_TABLET_MEMTABLE_SCANNER,
      PHY_TABLET_CACHE_JOIN,
      PHY_SAMPLE_MEMTABLE,

      PHY_MULTI_LOCAL_INDEX_SCAN,
      PHY_REMOTE_SSTABLE_SCAN,
      PHY_SINGLE_ROW_OP,
      PHY_UPS_SSTABLE_ENTITY_ITERATOR,
      PHY_REPLACE_FUSE,
      PHY_NULL_TO_NOP,

      PHY_MPI_DATA,
      PHY_MPI_SHUFFLE,
      PHY_TABLE_MPI_SCAN,
      PHY_HEAP_SORT,
      PHY_MPI_PSEUDO_SCAN,
      PHY_MPI_MERGE_JOIN,
      PHY_MPI_SCHEDULER,

      PHY_SERVER_STAT_RS,
      PHY_SERVER_STAT_UPS,
      PHY_SERVER_STAT_CS,
      PHY_SERVER_STAT_MS,
      PHY_LOCK_FILTER_IMPL,
      PHY_INC_SCAN_IMPL,
      PHY_INDEX_TRIGGER_IMPL,
      PHY_UPS_MODIFY_WITH_DML_TYPE_IMPL,
      PHY_TABLET_SCAN_V2_IMPL,
      PHY_TABLET_GET_V2_IMPL,
      PHY_MPI_SHUFFLE_CS,

      PHY_END /* end of phy operator type */
    '''
    phy_op_array = re.findall('^\s*PHY_([_A-Z0-9]+)', phy_op_def, re.M)
    if int(idx) < len(phy_op_array): return phy_op_array[int(idx)]
    else: return 'unknown'

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
