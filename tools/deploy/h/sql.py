from warnings import filterwarnings
import MySQLdb
filterwarnings('ignore', category = MySQLdb.Warning)

_sql_trace_ = False
class SqlConn:
    def __init__(self, conn_str, user='root', database='oceanbase'):
        logger.debug('make sql_conn: %s %s %s'%(conn_str, user, database))
        m = re.match('^(.*?):(.*?)$', conn_str)
        if not m: raise Fail('invalid conn str', conn_str)
        ip, port = m.groups()
        self.conn = MySQLdb.connect(host=ip, port=int(port), user=user, db=database)
    def commit(self):
        return self.conn.commit()
    def query(self, sql):
        cur = self.conn.cursor(MySQLdb.cursors.Cursor)
        cur.execute(sql)
        result = cur.fetchall()
        desc = cur.description
        if not desc:
            ret = [], []
        else:
            ret = [c[0] for c in desc], result
        cur.close()
        return ret

cur_sql_conn = None
    
def make_conn(server_path='obi.obs0', **kw):
    server = find(globals(), server_path)
    if not server: raise Fail('can not get_conn because "server" does not exist', server_path)
    conn = SqlConn(sub('$ip:$mysql_port', server), **kw)
    return conn

def use_conn(conn):
    global cur_sql_conn
    cur_sql_conn = conn

def get_cur_conn():
    global cur_sql_conn
    if not cur_sql_conn:
        cur_sql_conn = make_conn()
    return cur_sql_conn

def on_sql_warning(sql, e):
    logger.error("QueryFail: %s: %s"%(sql, e))
    
def on_sql_error(sql, e):
    logger.error("QueryFail: %s: %s"%(sql, e))
    if 'NoException' not in sql:
        raise e

def table_format(rows, header):
    if not rows:
        return 'Empty'
    def get_max_lens(rows):
        if not rows:
            return None
        col_lens = [1] * len(rows[0])
        for row in rows:
            for idx, col in enumerate(row):
                if len(str(col)) > col_lens[idx]:
                    col_lens[idx] = len(str(col))
        return col_lens
    def format_row(row, lens):
        return '|' + ''.join(" %-*s |"%(lens[idx], col != None and str(col) or '') for (idx,col) in enumerate(row))
    def ruler(lens):
        return '+' + ''.join('%s+'%('-' * (size + 2)) for size in lens)
    lens = get_max_lens(list(rows) + [header])
    lines = [ruler(lens)] + [format_row(header, lens)] + [ruler(lens)] + [format_row(row, lens) for row in rows] + [ruler(lens)]
    return '\n'.join(lines)

@Interp
def interp_sql(cmd, locals, globals, head='', ctx='void', **opt):
    out_list = ctx.startswith('for')
    def unlist(li):
        if len(li) == 1:
            return li[0]
        else:
            return li
    if re.search('[^a-zA-Z0-9]\$\w+', cmd):
        return None
    if head.lower() in 'use fsql set change show alter create drop insert update delete select commit'.split():
        if head == 'fsql':
            cmd = remove_head(cmd)
        try:
            if _sql_trace_:
                logger.info('sql: %s'%(cmd))
            header, rows = get_cur_conn().query(cmd)
            if ctx == 'void':
                result = table_format(rows, header)
            else:
                result = [unlist(row) for row in rows]
                if not out_list and len(result) == 1:
                    result = result[0]
            return dict(result=result)
        except MySQLdb.Error as e:
            on_sql_error(cmd, e)

@match_regist
def me_sql_target(head='', cmd='', type=None, **kw):
    if type != None: raise NotMatch()
    if head.lower() in 'use set change show alter create drop insert update delete select commit'.split():
        return E(kw, type='sql', head=head, cmd=cmd)
    else:
        raise NotMatch()

@match_regist
def me_sql_execute(cmd='', type='', ctx='void', **opt):
    match_by_rexp(type, 'sql')
    out_list = ctx.startswith('for')
    def unlist(li):
        if len(li) == 1:
            return li[0]
        else:
            return li
    try:
        if _sql_trace_:
            logger.info('sql: %s'%(cmd))
        header, rows = get_cur_conn().query(cmd)
        if ctx == 'void':
            result = table_format(rows, header)
        else:
            result = [unlist(row) for row in rows]
            if not out_list and len(result) == 1:
                result = result[0]
        return result
    except MySQLdb.Error as e:
        on_sql_error(cmd, e)
