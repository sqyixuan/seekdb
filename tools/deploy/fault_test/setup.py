tenant_guard = TenantGuard(tenant='tt1', database='faultdb')

def drop_table(**kw):
    with tenant_guard:
        drop table if exists t1;

def create_table(**kw):
    with tenant_guard:
        create table t1 (pk varchar(256) primary key);

def drop_and_create_table(**kw):
    drop_table
    create_table

def major_freeze(**kw):
    alter system major freeze;

def restart_observer(**kw):
    obi.rand_restart_observer

def switch_leader(table_name='t1', new_leader='', **kw):
    tid = get_table_id(table_name)
    logger.info('switch_new_leader: tname=%s tid=%d new_leader=%s'%(table_name, tid, new_leader))
    alter system switch replica leader partition_id='0%1@$tid' server='$new_leader'

def migrate(**kw):
    obi.rand_migrate

def get_table_id(table_name='t1', **kw):
    return select table_id from __all_table where table_name='$table_name';

def setup(**kw):
    CREATE RESOURCE UNIT testunit max_cpu 3, memory_size 2147483648;
    create resource pool testpool unit='testunit',unit_num=2,zone_list=('z1','z2','z3');
    create tenant tt1 replica_num =3,resource_pool_list=('testpool') set ob_tcp_invited_nodes='%';
    change effective tenant tt1;
    create database faultdb;
    change effective tenant sys;
    use oceanbase;
    create_table

def start_stress(**kw):
    s = Stress(user='root@tt1', database='faultdb', table='t1')
    checker = ThreadRunner(s.write)
    checker.start

ddl_count = loop_count
freeze_count = loop_count
switch_leader_count = loop_count
migrate_count = loop_count
restart_count = loop_count
mix_count = loop_count

