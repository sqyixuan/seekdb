#!/usr/bin/env python2
'''
Usages:
  ./deploy.py ... # equal ./hap.py ...
'''
import sys
import os
import subprocess
import json
import re
from datetime import datetime


class Reporter(object):

    def __init__(self, cmd):
        self.exec_time = None
        self.user = None
        self.cmd = cmd
        self.cost = -1
        self.code = 255
        self.source_branch = ''
        self.target_branch = ''
        self.is_interrupt = False
        self._base_dir = None
        self._prepare()

    @staticmethod
    def get_output(cmd):
        try:
            p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
            p.wait()
            if p.returncode == 0:
                return p.stdout.read()
        except:
            pass
        return ''

    @property
    def base_dir(self):
        if self._base_dir is None:
            self._base_dir = self.get_output("git rev-parse --show-toplevel").strip()
        return self._base_dir

    def set_cmd(self, cmd):
        self.cmd = cmd

    def _prepare(self):
        self.exec_time = datetime.now()
        self.user = os.environ.get('USER')
        if self.base_dir:
            source_path = os.path.join(self.base_dir, '.obdev/source_branch')
            target_path = os.path.join(self.base_dir, '.obdev/target_branch')
            self.source_branch = self.get_output('cat %s' % source_path).strip() + ':{}@{}'.format(self.base_dir, self.get_output('hostname -i').strip())
            self.target_branch = self.get_output('cat %s' % target_path).strip()

    def post(self):
        self.cost = (datetime.now() - self.exec_time).seconds
        self.exec_time = self.exec_time.strftime('%Y-%m-%d %H:%M:%S')
        data = json.dumps(dict((k, v) for k, v in vars(self).items() if not k.startswith('_')))
        ret = self.get_output(
            "curl -X 'POST' 'http://pre-gw.obrde.alibaba-inc.com/cmd-trace/api/v1/audit/add' "
            "-H 'accept: application/json' -H 'Content-Type: application/json' -m 2 -d '{}'".format(data))

def check_obd_version():
    if os.path.exists('.obd/version'):
        try:
            with open('../../deps/init/oceanbase.el7.x86_64.deps') as f:
                content = f.read()
            matched = re.search('\nob-deploy-([\S]+)-(\d+).el7.x86_64.rpm.*', content)
            if matched:
                version_in_deps = matched.group(1)
                release_in_deps = matched.group(2)
                deps_version_str = '{}.{}'.format(version_in_deps, release_in_deps)
                with open('.obd/version') as f:
                    version_str = f.read().strip()
                if deps_version_str != version_str:
                    obd_bin = os.path.join(_dep_dir, 'usr/bin/obd')
                    obd_version = Reporter.get_output('{} --version'.format(obd_bin))
                    matched = re.search('^OceanBase Deploy: (\S+)', obd_version)
                    if matched:
                        bin_version_str = matched.group(1)
                        if bin_version_str != deps_version_str:
                            print('\033[33m[WARN]\033[0m current obd version is not the latest version, use \'sh build.sh init\' to update')
        except:
            pass

def call(*popenargs, **kwargs):
    """Run command with arguments.  Wait for command to complete or
    timeout, then return the returncode attribute.

    The arguments are the same as for the Popen constructor.  Example:

    retcode = call(["ls", "-l"])
    """
    p = subprocess.Popen(*popenargs, **kwargs)
    try:
        return p.wait()
    except:  # Including KeyboardInterrupt, wait handled that.
        p.kill()
        p.wait()
        raise


def complete_opt_for_standalone(argv):
    run_cmd = argv[0]
    if ".mysqltest" in run_cmd or ".reboot" in run_cmd:
        if "--init-sql-files" not in argv or "init-sql-files" not in argv:
            argv.append("--init-sql-files='init.sql,init_user.sql\|root@sys\|test'")
        if ".mysqltest" in run_cmd and ("tenant" not in argv or "--tenant" not in argv or "-t" not in argv or "t" not in argv):
            argv.append("--tenant=sys")
    return argv
    
use_nhap = os.getenv('use_nhap') == '1'

_deploy_dir_ = os.path.dirname(os.path.realpath(__file__))
hap_file = use_nhap and 'nhap/hap.py' or 'hap.py'
extra_export_cmd = ''
home = os.getenv("HOME")
if os.path.exists(os.path.join(home, '.obd_business/.enable_global_ob_do')):
    ob_do_install_path = os.path.join(home, '.obd_business/exec')
    extra_export_cmd = 'export OBD_INSTALL_PRE={} &&'.format(ob_do_install_path)
    _deploy_cmd_ = os.path.join(ob_do_install_path, 'usr/bin/deploy_cmd')
else:
    _dep_dir = os.path.realpath(os.path.join(_deploy_dir_, '../..', 'deps/3rd'))
    _deploy_cmd_ = os.path.realpath(os.path.join(_dep_dir, 'usr/bin/deploy_cmd'))
if not os.path.exists(_deploy_cmd_):
    print("Please use 'sh build.sh init' to install deps.")
    sys.exit(1)
source_sh = os.path.realpath(os.path.join(_deploy_dir_, 'activate_obd.sh'))
default_switch = '0'
reporter = None
ret = 255
try:
    if os.getenv('CLASSIC_DEPLOY', default_switch) == '1':
        reporter = Reporter(cmd='./deploy.py ' + ' '.join(sys.argv[1:]))
        ret = subprocess.call([os.path.join(_deploy_dir_, hap_file)] + sys.argv[1:])
    else:
        check_obd_version()
        reporter = Reporter(cmd='./new_deploy.py ' + ' '.join(sys.argv[1:]))
        argv = complete_opt_for_standalone(sys.argv[1:])
        ret = call('source {} &&{}{} '.format(source_sh, extra_export_cmd, _deploy_cmd_) + ' '.join(argv), shell=True)
        
except KeyboardInterrupt:
    if reporter:
        reporter.is_interrupt = True
finally:
    if reporter:
        reporter.code = ret
        reporter.post()
sys.exit(ret)

