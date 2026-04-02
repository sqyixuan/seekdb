# coding: utf-8
# from __future__ import absolute_import, division, print_function
import datetime
import json
import logging
import os
import sys
from multiprocessing import Pool, current_process


def std_print(content):
    sys.stdout.write(content)
    sys.stdout.write('\n')
    sys.stdout.flush()


try:
    from arg_parser import parse_args, MERGED_FILE, set_log_level
except Exception as e:
    from core.arg_parser import parse_args, MERGED_FILE, set_log_level

__all__ = ('parse_file', 'scan_trace_file', 'create_merged_file')

if sys.version_info.major == 2:
    JsonDecodeError = ValueError
    _open = open


    def open(file, mode, encoding='utf-8'):
        return _open(file, mode)
else:
    JsonDecodeError = json.decoder.JSONDecodeError


def human_size(file):
    size = float(os.stat(file).st_size)
    if not size:
        return
    level = 0
    while size >= 1024:
        size = size / 1024
        level += 1
    mapping = {
        0: 'B',
        1: 'KB',
        2: 'MB',
        3: 'GB',
        4: 'TB'
    }
    return '{:.1f}{}'.format(size, mapping[level])


def parse_line(line, trace):
    traced = '{' + ('"trace_id":"%s"' % trace if trace else '')
    idx = line.find(traced)
    if idx == -1:
        return
    else:
        try:
            return json.loads(line[idx:-1])
        except JsonDecodeError as e:
            if line.endswith(']}\n'):
                new_line = line[idx:-3] + "...\"}]}"  # SQL too long causing truncated
            else:
                new_line = line[idx:-1] + '}'  # only wrote part of it
            try:
                return json.loads(new_line)
            except JsonDecodeError as e:
                return json.loads(line.replace('\t', '\\t')[idx:-5] + '..."}]}')  # ending with \\]} will still cause errors after replacement


def parse_log_file(file, trace):
    logging.info('pid: %s parse log file: %s' % (current_process(), file))
    counter = 0
    li = []
    with open(file, 'r', encoding='utf-8') as f:
        while True:
            line = f.readline()
            if line:
                parsed = parse_line(line, trace)
                if parsed:
                    counter += 1
                    li.append(parsed)
            else:
                logging.info('file:{} trace:{} total:{}'.format(file, trace, counter))
                break
    return li


def str_2_timestamp(t):
    if isinstance(t, int):
        return t
    temp = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f')
    return int(datetime.datetime.timestamp(temp) * 10 ** 6)


def parse_json_file(file, trace):
    key_mapping = {
        'span_id': 'id',
        'parent': 'parent_id',
        'span_name': 'name',
    }
    time_keys = ['start_ts', 'end_ts']

    def remap_key(di):
        for key, new_key in key_mapping.items():
            if key in di:
                temp = di[key]
                di[new_key] = temp
                di.pop(key)
        return di

    logging.info('pid: %s parse json file: %s' % (current_process(), file))
    li = []
    with open(file, 'r', encoding='utf-8') as f:
        content = f.read()
        data = json.loads(content)
        if not isinstance(data, list):
            raise ValueError('json file is not a list')
        for item in data:
            if trace == item['trace_id']:
                li.append(remap_key(item))
            for key in time_keys:
                item[key] = str_2_timestamp(item[key])
    return li


def parse_file(file, trace=''):
    std_print('parse file: {}'.format(file))
    if file.endswith('.json'):
        return parse_json_file(file, trace)
    else:
        return parse_log_file(file, trace)


def scan_trace_file(path='.'):
    prefix = 'trace.log'
    proxy_prefix = 'obproxy_trace.log'
    for entry in os.listdir(path):
        if entry.startswith(prefix) or entry.startswith(proxy_prefix):
            yield os.path.join(path, entry)


def dump_parsed_lines(li):
    loaded = parse_file(li[0], li[1])
    dumped = [json.dumps(line) + '\n' for line in loaded]
    return dumped


def file_mapping(args):
    trace_id = args.trace_id
    results = []
    for log_dir in args.log_dirs:
        if not os.path.isdir(log_dir):
            std_print('Dir not exist: {}'.format(log_dir))
            continue
        for file in scan_trace_file(log_dir):
            results.append((file, trace_id))
    for file in args.log_files:
        if not os.path.isfile(file):
            std_print('File not exist: {}'.format(file))
            continue
        if (file, trace_id) not in results:
            results.append((file, trace_id))
    return results


def create_merged_file(manual_args=None, merged_file=MERGED_FILE):
    args = parse_args(manual_args)
    set_log_level(args)
    file_mapped = file_mapping(args)
    if not file_mapped:
        logging.info('no valid file or dir: {}'.format(args))
        return
    logging.info('files: {}'.format(file_mapped))
    pool = Pool(min(6, len(file_mapped)))
    with open(merged_file, 'w', encoding='utf-8') as f:
        for result in pool.imap_unordered(dump_parsed_lines, file_mapped):
            f.writelines(result)
    pool.close()
    size = human_size(merged_file)
    if size:
        std_print('merged file created:{}'.format(size))
        return merged_file
    else:
        std_print('merged file is empty')


if __name__ == '__main__':
    create_merged_file()
