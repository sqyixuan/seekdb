# coding: utf-8
import argparse
import logging
import os
import posixpath

key_filename_path = posixpath.join(os.path.expanduser("~"), '.ssh', 'id_rsa')


def trace_arg_parser():
    parser = argparse.ArgumentParser(description='trace log')
    parser.add_argument('trace_id', metavar='trace_id', type=str, help='trace id to search')
    parser.add_argument('-d', '--log-dirs', nargs='+', default=[],
                        help='Format: -d path/to/local_log_dir @host[:port]/path/to/remote_log_dir  Note: remote path starts with @')
    parser.add_argument('-f', '--log-files', nargs='+', default=[],
                        help='Format: -f path/to/local_log_file @host[:port]/path/to/remote_log_files  Note: remote path starts with @')
    parser.add_argument('-u', '--username', type=str, default=None, help='username, required if requires remote file')
    parser.add_argument('-p', '--password', type=str, default=None, help='password')
    parser.add_argument('-k', '--key-filename', type=str, default=None, help='key-filename')
    parser.add_argument('-r', '--max-recursion', type=int, default=-1,
                        help='max recursion level, -1 means no limit. Default -1')
    parser.add_argument('--no-config', action='store_true', help='do not read from config option')
    parser.add_argument('--debug', type=str, default='WARNING', help='set debug level')
    return parser


def parse_args(args=None):
    parser = trace_arg_parser()
    return parser.parse_args(args)


def has_remote_request(args):
    for path in args.log_files:
        if path.startswith('@'):
            return True
    for path in args.log_dirs:
        if path.startswith('@'):
            return True
    return False


def input_password(args):
    from getpass import getpass
    while not args.username:
        username = input('请输入用户名>>')
        if username:
            args.username = username
    password = getpass("用户名:{}\n请输入密码>>".format(args.username))
    args.password = password


def set_log_level(args):
    mapping = {
        'CRITICAL': 50,
        'FATAL': 50,
        'ERROR': 40,
        'WARNING': 30,
        'WARN': 30,
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0,
    }
    if isinstance(args.debug, int):
        level = args.debug
    elif isinstance(args.debug, str):
        if args.debug.isdigit():
            level = int(args.debug)
        else:
            level = mapping.get(args.debug.upper(), 30)
    else:
        level = 30

    if level is not None:
        logging.basicConfig(level=level)
        args.debug = level
    else:
        print('Invalid debug option: {}'.format(args.debug))
        args.debug = 30


# constants
MERGED_FILE = 'trace_merged.log'
