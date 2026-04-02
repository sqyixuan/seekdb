#! /usr/bin/env python
# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------
# Filename:    clean_data.py
# Revision:    1.0
# Date:        2017/12/12
# Description: clean backup data
# -------------------------------------------------------------------------------
import os
import string
import sys
import shutil
import time
import traceback
import datetime
import logging
import logging.handlers
import re

handler = logging.handlers.RotatingFileHandler("log/clean_backup.log", maxBytes=1024 * 1024 * 128, backupCount=3)
fmt = '[%(asctime)s] %(levelname)s %(name)s (%(filename)s:%(lineno)s:%(lineno)s) %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)

logger = logging.getLogger('clean_backup')
logger.addHandler(handler)
logger.setLevel(logging.INFO)

    
# -------------------------------------------------------------------------------
# clean inc backup data
# -------------------------------------------------------------------------------
def get_backup_tenant_uri(base_dir, tenant_id):
    """
    Function to get tenant inc data path

    :arg base_dir:  backup cluster base dir
    :arg tenant_id: backup tenant_id

    :return: Path of tenant inc data path
    """
    return os.path.join(base_dir, "inc_data", str(tenant_id), '0')
           

def get_backup_tenant_index_file_uri(tenant_inc_data_path):
    """
    Function to get tenant index file path

    :arg tenant_inc_data_path: Side a of the triangle

    :return: Path of tenant index file path
    """
    return os.path.join(tenant_inc_data_path, "indexfile")

def get_del_inc_data(tenant_id, base_dir, min_data_version, min_freeze_timestamp):
    """
    Function to get del inc data

    :arg tenant_id:             backup tenant_id
    :arg base_dir:              backup cluster base dir
    :arg min_data_version:      unused now
    :arg min_freeze_timestamp:  backup cluster base dir

    :return: remove_file_id_array
    """
    #min_freeze_timestamp = min_freeze_timestamp / 1000 / 1000 # inc use seconds as timestamp
    # declare
    remove_file_id_array = []
    index_info_count = 0

    # 1. get tenant backup uri and index file uri
    backup_tenant_uri = get_backup_tenant_uri(base_dir, tenant_id)
    #logger.info("backup_tenant_uri", backup_tenant_uri)

    backup_tenant_index_file_uri = get_backup_tenant_index_file_uri(backup_tenant_uri)
    #logger.info("backup_tenant_index_file_uri", backup_tenant_index_file_uri)

    if os.path.exists(backup_tenant_index_file_uri):
        # 2. scan tenant index file and parse
        unneed_log_ids = set()
        try:
            findex = open(backup_tenant_index_file_uri)
            #logger.info("findex", findex)
    
            try:
                # handle index file
                for index_info in findex:
                    index_info_count += 1
                    #logger.info("index info", index_info)
    
                    # parse index info
                    parse_result = index_info.split(':')
                    parse_result_count = len(parse_result)
                    # logger.info(parse_result)
                    # logger.info("parse_result_count", parse_result_count)
    
                    # index info format
                    # file_id:name:value
                    # eg: 100:min_trans_ts":1510329600:"max_trans_ts":1510329700
                    # data_file_id=100 trans timestamp range [1510329600, 1510329700]
    
                    if parse_result_count != 5 :
                        raise Exception("InvalidIndexInfo", parse_result, parse_result_count)
                    else :
                        data_file_id_str = parse_result[0]
                        max_trans_ts_str = parse_result[4]
                        data_file_id = string.atoi(data_file_id_str)
                        max_trans_ts = string.atoi(max_trans_ts_str.replace("\r\n",""))
                        if max_trans_ts < min_freeze_timestamp:
                            logger.debug("add unneed log id, data_file_id={0} max_trans_ts={1} min_freeze_ts={2} ".format(
                                data_file_id, max_trans_ts, min_freeze_timestamp))
                            unneed_log_ids.add(data_file_id)
                        # logger.info(data_file_id, max_trans_ts_str, max_trans_ts)
                        # need to remove file id
            except "InvalidIndexInfo":
                logger.error("InvalidIndexInfo " + traceback.format_exc())
    
            finally:
                findex.close()
        except IOError as e:
            logger.error("ERROR, can not find index file or read index file fail: " + backup_tenant_index_file_uri + " " + traceback.format_exc())
        
        sub_files = os.listdir(backup_tenant_uri)
        del_log_ids = []
        prog = re.compile('(\d+)(.meta){0,1}')
        for sub_file in sub_files:
            if sub_file in ['indexfile', 'checkpoint']:
                continue  
            m = prog.match(sub_file)
            if m is None:
                logger.warn("skip unknown file " + sub_file)
            elif m.group(2) is None: # append log_id once
                log_id = int(m.group(1))
                if log_id in unneed_log_ids:
                    del_log_ids.append(log_id)
                    
        for del_log_id in sorted(del_log_ids):                   
            remove_data_file = os.path.join(backup_tenant_uri, str(del_log_id))
            remove_meta_file = remove_data_file + ".meta"
            if os.path.exists(remove_data_file):
                logger.info("add remove inc data: log_id={0} remove_data_file={1}".format(
                    del_log_id, remove_data_file))
                remove_file_id_array.append(remove_data_file)
            if os.path.exists(remove_meta_file):
                logger.info("add remove inc meta: log_id={0} remove_meta_file={1}".format(
                    del_log_id, remove_meta_file))
                remove_file_id_array.append(remove_meta_file)
         
        logger.info("min_data_version={0} index_info_count={1} del_count={2} min_freeze_timestamp={4} backup_tenant_index_file_uri={3}".format(
            min_data_version, index_info_count, len(remove_file_id_array), backup_tenant_index_file_uri, min_freeze_timestamp))       
    return remove_file_id_array

# -------------------------------------------------------------------------------
# clean base backup data
# -------------------------------------------------------------------------------
def get_tenant_ids(base_dir):
    tenant_ids = []
    tenant_id_file = os.path.join(base_dir, "all_tenant_id_list")
    with open(tenant_id_file) as f:
        for line in f:
            tmp = line.split(":")
            if len(tmp) > 3 or len(tmp) < 2:
                logger.error("invalid tenant_id " + line)
                sys.exit(1)
            tenant_ids.append(tmp[0].strip())
    return tenant_ids

def get_tenant_clean_info(base_dir, tenant_id, start_ts):
    backup_info_file = os.path.join(base_dir, "tenant_id", str(tenant_id), "backup_info")
    min_version = None
    min_freeze_ts = None
    with open(backup_info_file) as f:
        for line in f:
            tmp = line.split("_")
            if len(tmp) != 6:
                logger.error("invalid backup info " + line)
                sys.exit(1)
            tmp_version = int(tmp[1])
            tmp_freeze_ts = int(tmp[2])
            tmp_backup_result = tmp[3].strip()
            tmp_base_version = int(tmp[4])
            #logger.info("parse tenent clean info, file={0} version={1} freeze_ts={2} result={3}".format(backup_info_file, tmp_version, tmp_freeze_ts, tmp_backup_result))
            if tmp_backup_result == '0' and tmp_freeze_ts <= start_ts:
                if min_version is None or tmp_freeze_ts > min_freeze_ts:
                    min_version= tmp_base_version
                    min_freeze_ts = tmp_freeze_ts
    return (min_version, min_freeze_ts)

def get_del_base_data(tenant_id, base_dir, min_version):
    del_path_list = []
    sub_dirs = os.listdir(base_dir)
    sub_dir_names = []
    sub_base_data_dir_names = []
    for sub_dir in sub_dirs:
        try:
            version = int(sub_dir)
            sub_dir_names.append(version)
        except ValueError as e:
            tenant_path = os.path.join(base_dir, sub_dir)
            if sub_dir not in ['all_tenant_id_list', 'inc_data', 'tenant_id', 'inc_all_tenant_id_list']:
                logger.info("skip unknown path " + tenant_path)
                sub_base_data_dir_names.append(tenant_path)
    
    sub_dir_names = sorted(sub_dir_names)
    for version in sub_dir_names:
        tenant_path = os.path.join(base_dir, str(version), str(tenant_id))
        logger.info("check base data dir tenant_id={0} version={1} need_del={2}".format(
            tenant_id, version, version < min_version))
        if version < min_version:
            if os.path.exists(tenant_path):
                logger.info("add into del list, "+ tenant_path)
                del_path_list.append(tenant_path)
    for base_data_history_dir in sub_base_data_dir_names:
        base_dirs_level_arr = base_data_history_dir.split("/")
        base_dir_lowest_level = base_dirs_level_arr[len(base_dirs_level_arr) - 1]
        base_version = base_dir_lowest_level.split("_")[2]
        if int(base_version) < min_version:
            del_base_data_dir = os.path.join(base_data_history_dir, str(tenant_id))
            logger.info("add into del list, "+ del_base_data_dir)
            del_path_list.append(del_base_data_dir)
    return del_path_list

def get_tenant_del_path_list(base_dir, tenant_id, min_version, min_freeze_ts):
    tenant_del_files = get_del_inc_data(tenant_id, base_dir, min_version, min_freeze_ts)
    tenant_del_dirs = get_del_base_data(tenant_id, base_dir, min_version)
    logger.info("get_tenant_del_path_list, tenant_id={0} min_version={3} min_freeze_ts={4} inc_file={1} base_file={2}".format(
        tenant_id, len(tenant_del_files), len(tenant_del_dirs), min_version, min_freeze_ts))
    return (tenant_del_files, tenant_del_dirs)

def clean_empty_base_data_dir(base_dir, min_version):    
    sub_dirs = os.listdir(base_dir)
    sub_base_data_dir_names = []
    for sub_dir in sub_dirs:
        sub_path = os.path.join(base_dir, sub_dir)
        try:
            version = int(sub_dir)
            if version < min_version and len(os.listdir(sub_path)) == 0:
                logger.info("del empty dir " + sub_path)
                os.rmdir(sub_path)
        except ValueError as e:
            sub_dir_path = os.path.join(base_dir, sub_dir)
            if sub_dir not in ['all_tenant_id_list', 'inc_data', 'tenant_id', 'inc_all_tenant_id_list']:
                sub_base_data_dir_names.append(sub_dir_path)
    for base_data_history_dir in sub_base_data_dir_names:
        base_dirs_level_arr = base_data_history_dir.split("/")
        base_dir_lowest_level = base_dirs_level_arr[len(base_dirs_level_arr) - 1]
        base_version = base_dir_lowest_level.split("_")[2]
        if int(base_version) < min_version:
            if len(os.listdir(base_data_history_dir)) == 0:
                logger.info("del empty dir " + base_data_history_dir)
                os.rmdir(base_data_history_dir)
        
def get_min_version(base_dir, tenant_id_list, start_ts):    
    min_version = None
    min_freeze_ts = None
    
    for tenant_id in tenant_id_list:
        (tmp_min_version, tmp_min_freeze_ts) = get_tenant_clean_info(base_dir, tenant_id, start_ts)
        if tmp_min_freeze_ts is None:
            logger.info("some tenant has not success backup, cannot clean, tenant_id={0} start_ts={1} base_dir={2}".format(
                tenant_id, start_ts, base_dir))
            min_version = None
            min_freeze_ts = None    
            break
        if min_freeze_ts is None or tmp_min_freeze_ts < min_freeze_ts:
            min_version = tmp_min_version
            min_freeze_ts = tmp_min_freeze_ts
    return (min_version, min_freeze_ts)

def do_main(base_dir, start_ts, try_run):
    del_file_list = []
    del_dir_list = []
    tenant_id_list = get_tenant_ids(base_dir)
    (min_version, min_freeze_ts) = get_min_version(base_dir, tenant_id_list, start_ts)
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(min_freeze_ts/1000000.0))
    logger.info("start to clean backup data, start_ts={0} keep_day_num={1} base_dir={2} try_run={3} min_version={5} min_freeze_ts={6} ({7})".format(
        start_ts, keep_day_num, base_dir, try_run, datetime.datetime.now(), min_version, min_freeze_ts, time_str))
    logger.info("tenant_id_list={0}".format(tenant_id_list))
    
    if min_version is not None:
        for tenant_id in tenant_id_list:
            try:
                (tenant_del_files, tenant_del_dirs) = get_tenant_del_path_list(base_dir, tenant_id, min_version, min_freeze_ts)
                del_file_list += tenant_del_files
                del_dir_list += tenant_del_dirs
            except BaseException as e:
                logger.warn("failed to get del file list, tenant_id={0} e={1} back={2}".format(tenant_id, e, traceback.format_exc()))                
        
        logger.info("del path list, try_run={0}, del_inc_data={1}, del_base_data={2} min_version={3}:".format(
            try_run, len(del_file_list), len(del_dir_list), min_version))
        for path in del_file_list:
            logger.info("del inc data: " + path)
            if not try_run:
                os.unlink(path)
        for path in del_dir_list:
            logger.info("del base data: " + path)
            if not try_run:
                try:
                    shutil.rmtree(path)
                except Exception:
                    logger.warn("warn={1}".format(sys.argv, traceback.format_exc()))
        
        clean_empty_base_data_dir(base_dir, min_version)
        
if __name__ == '__main__':
    if len(sys.argv) != 3 and len(sys.argv) != 4:
        print("invalid args {0}, run python clean_backup.py <backup_dir> <keep_day_num> [try_run]".format(sys.argv))
        sys.exit(1)

    base_dir = sys.argv[1]
    keep_day_num = int(sys.argv[2])
    try_run = False 
    if len(sys.argv) == 4:
        try_run = True
        
    if keep_day_num <= 0:
        print("invalid keep_day_num {0}".format(keep_day_num))
        sys.exit(1)
    if os.path.exists(base_dir) is False:
        print("{0} not exist".format(base_dir))
        sys.exit(1)
    if base_dir[0] != '/':
         print("base_dir must be absolute path, base_dir=" + base_dir)
         sys.exit(1)
    
    start_ts = int(time.time() * 1000000) - keep_day_num * 24 * 3600 * 1000000 # us        
    try:
        do_main(base_dir, start_ts, try_run)
    except BaseException as e:
        logger.error("failed to clean backup data, argv={0} error={1}".format(sys.argv, traceback.format_exc()))
