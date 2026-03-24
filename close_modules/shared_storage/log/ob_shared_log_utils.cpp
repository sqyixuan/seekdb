/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ob_shared_log_utils.h"
#include "share/object_storage/ob_device_config_mgr.h"                // ObDeviceConfigMgr
#include "share/backup/ob_backup_io_adapter.h"                        // ObBackupIoAdapter
#include "logservice/palf/log_block_header.h"                         // LogBlockHeader
#include "logservice/palf/log_io_context.h"                           // LogIOContext
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
namespace logservice
{
#define CHECK_SHARED_STORAGE_WHTHER_ENABLED \
  if (!GCTX.is_shared_storage_mode()) {  \
    CLOG_LOG_RET(ERROR, OB_IO_ERROR, "shared storage not enable");\
    return OB_IO_ERROR;\
  }

int ObSharedLogUtils::get_oldest_block(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  palf::block_id_t &oldest_block_id)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.get_oldest_block(tenant_id, ls_id, oldest_block_id);
}

int ObSharedLogUtils::get_newest_block(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  palf::block_id_t &out_block_id)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.get_newest_block(tenant_id, ls_id, start_block_id, out_block_id);
}


int ObSharedLogUtils::locate_by_scn_coarsely(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  const palf::block_id_t &end_block_id,
  const share::SCN &scn,
  palf::block_id_t &out_block_id,
  share::SCN &out_block_min_scn)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.locate_by_scn_coarsely(tenant_id, ls_id, start_block_id, end_block_id,
                                                        scn, out_block_id, out_block_min_scn);
}

int ObSharedLogUtils::get_block_min_scn(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &block_id,
  share::SCN &block_min_scn)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.get_block_min_scn(tenant_id, ls_id, block_id, block_min_scn);
}

int ObSharedLogUtils::delete_blocks(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  const palf::block_id_t &end_block_id)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.delete_blocks(tenant_id, ls_id, start_block_id, end_block_id);
}

int ObSharedLogUtils::construct_external_storage_access_info(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &block_id,
  char *out_uri,
  const int64_t out_uri_buf_len,
  ObBackupDest &dest,
  uint64_t &storage_id)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.construct_external_storage_access_info(
    tenant_id, ls_id, block_id, out_uri, out_uri_buf_len, dest, storage_id);
}

int ObSharedLogUtils::check_ls_exist(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  bool &ls_exist)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.check_ls_exist(tenant_id, ls_id, ls_exist);
}

int ObSharedLogUtils::check_tenant_exist(
  const uint64_t tenant_id,
  bool &tenant_exist)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.check_tenant_exist(tenant_id, tenant_exist);
}

int ObSharedLogUtils::delete_tenant(
  const uint64_t tenant_id)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.delete_tenant(tenant_id);
}

int ObSharedLogUtils::delete_ls(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id)
{
  CHECK_SHARED_STORAGE_WHTHER_ENABLED;
  return SHARED_LOG_GLOBAL_UTILS.delete_ls(tenant_id, ls_id);
}
// use ObBackupIoAdapter to access object storage.
// ======================================= Implementions =============================================
ObSharedLogDirOp::ObSharedLogDirOp()
  : ObBaseDirEntryOperator()
{
}

ObSharedLogDirOp::~ObSharedLogDirOp()
{
}

int ObSharedLogDirOp::set_used_for_marker(
  const char *marker,
  const int64_t limited_num,
  const UserFunction &user_function)
{
  int ret = OB_SUCCESS;
  if (0 >= limited_num || !user_function.is_valid() || OB_ISNULL(marker)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments!", K(limited_num), K(user_function), KP(marker));
  } else if (OB_FAIL(ObBaseDirEntryOperator::set_marker_flag(marker, limited_num))) {
    CLOG_LOG(WARN, "set_marker_flag failed", K(limited_num), K(user_function), K(marker));
  } else {
    function_ = user_function;
  }
  return ret;
}

int ObSharedLogDirOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ret = function_(entry->d_name);
  return ret;
}

const char *ObSharedLogUtilsImpl::TENANT_FORMAT = "%s/cluster_%ld/tenant_%ld/clog";
const char *ObSharedLogUtilsImpl::LS_FORMAT = "%s/cluster_%ld/tenant_%ld/clog/%ld/";
const char *ObSharedLogUtilsImpl::BLOCK_FORMAT = "%s/cluster_%ld/tenant_%ld/clog/%ld/%012ld";
const int64_t ObSharedLogUtilsImpl::DEFAULT_BATCH_COUNT = 1;
const int64_t ObSharedLogUtilsImpl::RIGHT_ALIGN_COUNT = 12;


ObSharedLogUtilsImpl& ObSharedLogUtilsImpl::get_instance()
{
  static ObSharedLogUtilsImpl impl;
  return impl;
}

int ObSharedLogUtilsImpl::get_oldest_block(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  palf::block_id_t &oldest_block_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_oldest_block_impl_(tenant_id, ls_id, oldest_block_id))) {
    CLOG_LOG(WARN, "get_oldest_block failed", K(tenant_id), K(ls_id), K(oldest_block_id));
  } else {
    CLOG_LOG(INFO, "get_oldest_block sucess", K(tenant_id), K(ls_id), K(oldest_block_id));
  }
  return ret;
}

int ObSharedLogUtilsImpl::get_newest_block(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  palf::block_id_t &out_block_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !is_valid_block_id(start_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id), K(start_block_id));
  } else if (OB_FAIL(get_newest_block_impl_(tenant_id, ls_id, start_block_id, out_block_id))) {
    CLOG_LOG(WARN, "get_newest_block_impl_ failed", K(tenant_id), K(ls_id), K(start_block_id), K(out_block_id));
  } else {
    CLOG_LOG(INFO, "get_newest_block sucess", K(tenant_id), K(ls_id), K(start_block_id), K(out_block_id));
  }
  return ret;
}

int ObSharedLogUtilsImpl::locate_by_scn_coarsely(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  const palf::block_id_t &end_block_id,
  const share::SCN &scn,
  palf::block_id_t &out_block_id,
  share::SCN &out_block_min_scn)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !is_valid_block_id(start_block_id) 
      || !is_valid_block_id(end_block_id)
      || start_block_id >= end_block_id
      || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id), K(start_block_id), K(end_block_id), K(scn));
  } else if (OB_FAIL(binary_search_block_by_scn_(tenant_id, ls_id, scn, start_block_id, 
                                                 end_block_id, out_block_id, out_block_min_scn))){
    CLOG_LOG(WARN, "required block doesn't exist", K(tenant_id), K(ls_id), K(start_block_id), K(end_block_id));
  } else {
    CLOG_LOG(INFO, "find block successfully", K(tenant_id), K(ls_id), K(out_block_id), K(out_block_min_scn));
  }

  return ret;
}

int ObSharedLogUtilsImpl::get_block_min_scn(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &block_id,
  share::SCN &block_min_scn)
{
  // 1. get uri by construct_block_str_ by specific block_id 
  // 2. get block min scn by specific uri
  int ret = OB_SUCCESS;
  LogBlockHeader block_header;
  if (!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !is_valid_block_id(block_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id), K(block_id));
  } else if (OB_FAIL(get_block_header_(tenant_id, ls_id, block_id, block_header))) {
    CLOG_LOG(WARN, "get_block_header_ failed", K(tenant_id), K(ls_id), K(block_id));
  } else {
    block_min_scn = block_header.get_min_scn();
    CLOG_LOG(TRACE, "get_block_header_ success", K(tenant_id), K(ls_id), K(block_id), K(block_header));
  }
  return ret;
}

int ObSharedLogUtilsImpl::delete_blocks(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  const palf::block_id_t &end_block_id)
{
  // 1. get uris in the range [start_block_id, end_block_id)
  // 2. delete each block in uris.
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !is_valid_block_id(start_block_id)
      || !is_valid_block_id(end_block_id)
      || start_block_id >= end_block_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id), K(start_block_id), K(end_block_id));
  } else {
    int64_t remained_count = end_block_id - start_block_id;
    block_id_t current_round_start_block_id = start_block_id;
    while (remained_count > 0 && OB_SUCC(ret)) {
      int64_t current_round_to_be_deleted_count = MIN(DEFAULT_BATCH_COUNT, remained_count);
      if (OB_FAIL(delete_blocks_impl_(tenant_id, ls_id,
                                      current_round_start_block_id,
                                      current_round_to_be_deleted_count))) {
        CLOG_LOG(WARN, "delete_blocks_impl_ failed", K(tenant_id), K(ls_id),
                 K(current_round_start_block_id), K(current_round_to_be_deleted_count));
      }
      remained_count -= current_round_to_be_deleted_count;
      current_round_start_block_id += current_round_to_be_deleted_count;
    }
    CLOG_LOG(INFO, "delete_blocks finished", K(tenant_id), K(ls_id), K(start_block_id), K(end_block_id));
  }
  return ret;
}

// uri is oss://ABC/DEF/marker
// uri is splited to dir_path = oss://ABC/DEF, marker = marker
int split_marker_and_dir_path(char *uri, char *&marker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uri)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "uri is NULL", K(ret), K(uri));
  } else if (OB_ISNULL(marker = strrchr(uri, '/'))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "uri is has no '/'", K(ret), K(uri));
  } else {
    *marker = '\0';
    marker += 1;
  }
  return ret;
}

int ObSharedLogUtilsImpl::check_tenant_exist(
  const uint64_t tenant_id,
  bool &tenant_exist)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  const int64_t limited_num = 1;
  int64_t objects = 0;
  char *marker =  nullptr;
  ObSharedLogDirOp::UserFunction function = [&objects](const char *uri) {
    objects++;
    return OB_SUCCESS;
  };
  tenant_exist = false;
  char uri[OB_MAX_URI_LENGTH] = { '\0' };
  ObLSID min_ls_id(0);
  uint64_t storage_id = UINT64_MAX;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id));
  } else if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id)); 
    // uri is oss://example_bucket/clusterxx/clog/tenant_xx/0/
  } else if (OB_FAIL(construct_ls_str_(dest, tenant_id, min_ls_id, uri, sizeof(uri)))) {
    CLOG_LOG(WARN, "construct_block_str_ failed", K(tenant_id));
    // make uri to oss://example_bucket/clusterxx/clog/tenant_xx/0
  } else if (OB_FAIL(convert_last_non_zero_char_to_zero_(uri))) {
    CLOG_LOG(WARN, "convert_last_non_zero_char_to_zero_ failed", KP(uri));
  } else if (OB_FAIL(split_marker_and_dir_path(uri, marker))) {
    CLOG_LOG(WARN, "split_marker_and_dir_path failed", K(ret), KP(uri));
  } else if (OB_FAIL(list_blocks_impl_(dest, uri, marker, limited_num, function))) {
    CLOG_LOG(WARN, "list_blocks_impl_ failed", K(tenant_id), K(ret), K(uri), K(marker));
  } else if (0 == objects) {
    CLOG_LOG(WARN, "tenant not exists on shared storage", K(ret), K(tenant_id), K(uri), K(marker));
  } else {
    tenant_exist = true;
    CLOG_LOG(TRACE, "check_tenant_exist success", K(tenant_id), K(objects));
  }
  return ret;
}

int ObSharedLogUtilsImpl::check_ls_exist(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  bool &ls_exist)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  const int64_t limited_num = 1;
  int64_t objects = 0;
  char *marker = nullptr;
  ObSharedLogDirOp::UserFunction function = [&objects](const char *uri) {
    objects++;
    return OB_SUCCESS;
  };
  char uri[OB_MAX_URI_LENGTH] = { '\0' };
  block_id_t min_block_id = 0;
  ls_exist = false;
  uint64_t storage_id = UINT64_MAX;
  if (!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id)); 
  } else if (OB_FAIL(construct_block_str_(dest, tenant_id, ls_id, min_block_id, uri, sizeof(uri)))) {
    CLOG_LOG(WARN, "construct_block_str_ failed", K(tenant_id), K(ls_id));
    // We need make last char to '\0', otherwise, we can not count the block is '0000 0000 0000'.
  } else if (OB_FAIL(convert_last_non_zero_char_to_zero_(uri))) {
    CLOG_LOG(WARN, "convert_last_non_zero_char_to_zero_ failed", KP(uri));
  } else if (OB_FAIL(split_marker_and_dir_path(uri, marker))) {
    CLOG_LOG(WARN, "split_marker_and_dir_path failed", K(ret), KP(uri));
  } else if (OB_FAIL(list_blocks_impl_(dest, uri, marker, limited_num, function))) {
    CLOG_LOG(WARN, "list_blocks_impl_ failed", K(tenant_id), K(ls_id), K(ret), K(marker), K(uri));
  } else if (0 == objects) {
    CLOG_LOG(WARN, "palf not exists on shared storage", K(tenant_id), K(ls_id), K(uri), K(ret), K(marker)); 
  } else {
    ls_exist = true;
    CLOG_LOG(TRACE, "check_ls_exist success", K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObSharedLogUtilsImpl::delete_tenant(
  const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char uri[OB_MAX_URI_LENGTH] = { '\0' };
  ObBackupDest dest;
  ObBackupIoAdapter adapter;
  uint64_t storage_id = UINT64_MAX;
  const bool need_recursive = true;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id));
  } else if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id));
  } else if (OB_FAIL(construct_tenant_str_(dest, tenant_id, uri, sizeof(uri)))) {
    CLOG_LOG(WARN, "construct_tenant_str_ failed", K(tenant_id));
  } else if (OB_FAIL(adapter.del_dir(uri, dest.get_storage_info(), need_recursive))) {
    CLOG_LOG(WARN, "del_dir failed", K(tenant_id), K(uri));
    ret = convert_ret_code_(ret);
  } else {
    CLOG_LOG(INFO, "delete_tenant success", K(tenant_id), K(uri));
  }
  return ret;
}

int ObSharedLogUtilsImpl::delete_ls(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  char uri[OB_MAX_URI_LENGTH] = { '\0' };
  ObBackupDest dest;
  ObBackupIoAdapter adapter;
  uint64_t storage_id = UINT64_MAX;
  const bool need_recursive = true;
  if (!is_valid_tenant_id(tenant_id) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id));
  } else if (OB_FAIL(construct_ls_str_(dest, tenant_id, ls_id, uri, sizeof(uri)))) {
    CLOG_LOG(WARN, "construct_ls_str_ failed", K(tenant_id), K(ls_id));
  } else if (OB_FAIL(adapter.del_dir(uri, dest.get_storage_info(), need_recursive))) {
    CLOG_LOG(WARN, "del_dir failed", K(tenant_id), K(ls_id), K(uri));
    ret = convert_ret_code_(ret);
  } else {
    CLOG_LOG(INFO, "delete_ls success", K(tenant_id), K(ls_id), K(uri));
  }
  return ret;
}

int ObSharedLogUtilsImpl::construct_external_storage_access_info(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &block_id,
  char *out_uri,
  const int64_t out_uri_buf_len,
  ObBackupDest &dest,
  uint64_t &storage_id)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)
      || !ls_id.is_valid()
      || !is_valid_block_id(block_id)
      || OB_ISNULL(out_uri)
      || 0 >= out_uri_buf_len) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(ls_id), K(block_id), K(out_uri), K(out_uri_buf_len));
  } else if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id), K(block_id), K(out_uri));
  } else if (OB_FAIL(construct_block_str_(dest, tenant_id, ls_id, block_id, out_uri, out_uri_buf_len))) {
    CLOG_LOG(WARN, "construct_block_str_ failed", K(tenant_id), K(ls_id), K(block_id), K(out_uri));
  } else {
    CLOG_LOG(TRACE, "construct_external_storage_access_info success", K(tenant_id), K(ls_id),
             K(block_id), K(out_uri));
  }
  return ret;
}

int ObSharedLogUtilsImpl::get_oldest_block_impl_(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  palf::block_id_t &oldest_block_id)
{
  int ret = OB_SUCCESS;
  char uri[OB_MAX_URI_LENGTH] = { '\0' };
  oldest_block_id = LOG_MAX_BLOCK_ID;
  ObBackupDest dest;
  int64_t objects = 0;
  ObSharedLogDirOp::UserFunction user_function = [&oldest_block_id,
                                                  &objects](const char *uri) {
    block_id_t curr_block_id = LOG_INVALID_BLOCK_ID;
    int ret = OB_SUCCESS;
    if (OB_ISNULL(uri) || RIGHT_ALIGN_COUNT != strlen(uri)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", KP(uri));
    } else if (OB_FAIL(ob_atoull(uri, curr_block_id))) {
      CLOG_LOG(WARN, "ob_atoull failed", K(uri));
    } else if (!palf::is_valid_block_id(curr_block_id)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid block id", K(uri), K(curr_block_id));
    } else if (oldest_block_id > curr_block_id) {
      oldest_block_id = curr_block_id;
    } else {
      CLOG_LOG(INFO, "execute success", K(uri), K(oldest_block_id), K(curr_block_id));
    }
    objects++;
    return ret;
  };
  block_id_t marker = palf::LOG_INITIAL_BLOCK_ID;
  char *marker_str = nullptr;
  const int64_t limited_num = 1;
  uint64_t storage_id = UINT64_MAX;
  if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id), K(marker));
  } else if (OB_FAIL(construct_block_str_(dest, tenant_id, ls_id, marker, uri, sizeof(uri)))) {
    CLOG_LOG(WARN, "construct_block_str_ failed", K(tenant_id), K(ls_id), K(marker));
    // reduce list cost via list with marker, however, the interface provided by SDK just only return the object
    // whose name are alphabetically greater than marker(not include marker). i.e. marker is 0000 0000 0000,
    // the result of list will not include this object even if it exists.
    // 
    // to solve above problem, we need set the last char of uri to '\0'(marker is 0000 0000 000).
  } else if (OB_FAIL(convert_last_non_zero_char_to_zero_(uri))) {
    CLOG_LOG(WARN, "convert_last_non_zero_char_to_zero_ failed", KP(uri));
  } else if (OB_FAIL(split_marker_and_dir_path(uri, marker_str))) {
    CLOG_LOG(WARN, "split_marker_and_dir_path failed", K(ret), KP(uri));
  } else if (OB_FAIL(list_blocks_impl_(dest, uri, marker_str, limited_num, user_function))) {
    CLOG_LOG(WARN, "list_blocks_impl_ failed", K(uri), K(ret), K(marker_str));
  } else if (0 == objects) {
    CLOG_LOG(WARN, "there is no blocks on shared storage", K(uri), K(marker_str));
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObSharedLogUtilsImpl::get_newest_block_impl_(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  palf::block_id_t &newest_block_id)
{
  int ret = OB_SUCCESS;
  char uri[OB_MAX_URI_LENGTH] = { '\0' };
  ObBackupDest dest;
  newest_block_id = LOG_INITIAL_BLOCK_ID;
  int64_t objects = 0;
  ObSharedLogDirOp::UserFunction function = [&start_block_id,
                                             &newest_block_id,
                                             &objects](const char *uri) {
    block_id_t curr_block_id = LOG_INVALID_BLOCK_ID;
    int ret = OB_SUCCESS;
    if (OB_ISNULL(uri) || RIGHT_ALIGN_COUNT != strlen(uri)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", KP(uri));
    } else if (OB_FAIL(ob_atoull(uri, curr_block_id))) {
      CLOG_LOG(WARN, "ob_atoull failed", K(uri));
    } else if (!palf::is_valid_block_id(curr_block_id)) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid block id", K(uri), K(curr_block_id));
    } else if (curr_block_id >= start_block_id && curr_block_id >= newest_block_id) {
      newest_block_id = curr_block_id;
      objects++;
      CLOG_LOG(TRACE, "execute success", K(uri), K(newest_block_id), K(curr_block_id), K(start_block_id));
    } else {
    }
    return ret;
  };
  bool is_initial_block_id = (LOG_INITIAL_BLOCK_ID == start_block_id);
  block_id_t marker = (is_initial_block_id ? start_block_id : start_block_id - 1);
  char *marker_str = nullptr;
  const int64_t limited_num = INT64_MAX;
  uint64_t storage_id = UINT64_MAX;
  if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id), K(start_block_id), K(marker));
  } else if (OB_FAIL(construct_block_str_(dest, tenant_id, ls_id, marker, uri, sizeof(uri)))) {
    CLOG_LOG(WARN, "construct_block_str_ failed", K(tenant_id), K(ls_id), K(start_block_id), K(marker));
  } else if(is_initial_block_id
      && OB_FAIL(convert_last_non_zero_char_to_zero_(uri))) {
    CLOG_LOG(WARN, "convert_last_non_zero_char_to_zero_ failed", K(uri));
  } else if (OB_FAIL(split_marker_and_dir_path(uri, marker_str))) {
    CLOG_LOG(WARN, "split_marker_and_dir_path failed", K(ret), KP(uri));
  } else if (OB_FAIL(list_blocks_impl_(dest, uri, marker_str, limited_num, function))) {
    CLOG_LOG(WARN, "list_blocks_impl_ failed", K(tenant_id), K(ls_id), K(marker), K(limited_num),
        K(ret), K(marker_str));
  } else if (0 == objects) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "there is no blocks on shared storage", K(uri), K(ret), K(marker_str));
  }
  return ret;
}

int ObSharedLogUtilsImpl::list_blocks_impl_(
  const ObBackupDest &dest,
  const char *uri,
  const char *marker,
  const int64_t limited_num,
  ObSharedLogDirOp::UserFunction &user_function)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObSharedLogDirOp op;
  int64_t objects = 0;

  if (OB_ISNULL(marker)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "marker is null", K(ret), K(uri), K(marker));
  } else if (OB_UNLIKELY(!user_function.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "user_function is invalid", K(uri), K(limited_num));
    // set marker which used for list_files.
    // list_files just return the file whose names is alphabetically greater than the marker(not include marker).
  } else if (OB_FAIL(op.set_used_for_marker(marker, limited_num, user_function))) {
    CLOG_LOG(WARN, "set ob_object_storage_struct failed", K(uri));
  } else if (OB_FAIL(adapter.adaptively_list_files(uri, dest.get_storage_info(), op))) {
    CLOG_LOG(WARN, "adaptively_list_files failed", K(uri), K(op), K(ret), K(marker), K(limited_num));
    ret = convert_ret_code_(ret);
    // NB: for object storage, there is no possibility that return OB_ENTRY_NOT_EXIST even if there is no block
    //     whose prefix match uri.
    if (OB_ENTRY_NOT_EXIST == ret) {
      ObString uri_ob_string(0, strlen(uri), uri);
      if (uri_ob_string.prefix_match(OB_FILE_PREFIX)) {
        ret = OB_SUCCESS;
      }
    } else {
      CLOG_LOG(WARN, "list_blocks_impl_ failed", K(uri));
    }
  } else {
    CLOG_LOG(INFO, "list_files with marker success", K(uri), K(op), K(objects), K(marker));
  }
  return ret;
}

int ObSharedLogUtilsImpl::construct_tenant_str_(
  const share::ObBackupDest &dest,
  const uint64_t tenant_id,
  char *out_str,
  const int64_t out_str_buf_len)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  const int64_t cluster_id = GCONF.cluster_id;
  if (0 >= (pret = snprintf(out_str, out_str_buf_len, TENANT_FORMAT,
                            dest.get_root_path().ptr(), cluster_id, tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "snprintf failed", K(pret), K(tenant_id));
    } else {
    CLOG_LOG(TRACE, "construct_tenant_str_ success", K(pret), K(tenant_id), K(out_str));
  }
  return ret;
}

int ObSharedLogUtilsImpl::construct_ls_str_(
  const share::ObBackupDest &dest,
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  char *out_str,
  const int64_t out_str_buf_len)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  const int64_t cluster_id = GCONF.cluster_id;
  if (0 >= (pret = snprintf(out_str, out_str_buf_len, LS_FORMAT,
                            dest.get_root_path().ptr(), cluster_id, tenant_id, ls_id.id()))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "snprintf failed", K(pret), K(tenant_id), K(ls_id));
    } else {
    CLOG_LOG(TRACE, "construct_ls_str_ success", K(pret), K(tenant_id), K(ls_id),
             K(out_str));
  }
  return ret;
}

int ObSharedLogUtilsImpl::construct_block_str_(
  const share::ObBackupDest &dest,
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &block_id,
  char *out_str,
  const int64_t out_str_buf_len)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  const int64_t cluster_id = GCONF.cluster_id;
  if (0 >= (pret = snprintf(out_str, out_str_buf_len, BLOCK_FORMAT,
                            dest.get_root_path().ptr(), cluster_id, tenant_id, ls_id.id(), block_id))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "snprintf failed", K(pret), K(tenant_id), K(ls_id), K(block_id));
    } else {
    CLOG_LOG(TRACE, "construct_block_str_ success", K(pret), K(tenant_id), K(ls_id),
             K(block_id), K(out_str));
  }
  return ret;
}

int ObSharedLogUtilsImpl::get_block_header_(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &block_id,
  LogBlockHeader &block_header)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObBackupDest dest;
  char uri[OB_MAX_URI_LENGTH] = { '\0' };
  char *read_buf = reinterpret_cast<char *>(ob_malloc(MAX_INFO_BLOCK_SIZE, "ObLogExtU"));
  int64_t read_size = 0;
  int64_t pos = 0;
  uint64_t storage_id = UINT64_MAX;
  palf::LogIOContext io_ctx(tenant_id, ls_id.id(), palf::LogIOUser::META_INFO);
  CONSUMER_GROUP_FUNC_GUARD(io_ctx.get_function_type());
  if (OB_ISNULL(read_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate read buf failed", K(tenant_id), K(ls_id), K(block_id));
  } else if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id), K(block_id));
  } else if (OB_FAIL(construct_block_str_(dest, tenant_id, ls_id, block_id, uri, sizeof(uri)))) {
    CLOG_LOG(WARN, "construct_block_str_ failed", K(tenant_id), K(ls_id), K(block_id));
  } else if (OB_FAIL(adapter.pread(uri, dest.get_storage_info(), read_buf, MAX_INFO_BLOCK_SIZE,
                                   0, read_size,
                                   common::ObStorageIdMod(storage_id, common::ObStorageUsedMod::STORAGE_USED_CLOG)))) {
    CLOG_LOG(WARN, "pread failed", K(uri), K(tenant_id), K(ls_id), K(block_id));
    ret = convert_ret_code_(ret);
  } else if (OB_FAIL(block_header.deserialize(read_buf, read_size, pos))) {
    CLOG_LOG(WARN, "deserialize failed", K(uri), K(tenant_id), K(ls_id), K(block_id));
  } else {
    CLOG_LOG(INFO, "get_block_header_ success", K(uri), K(block_header));
  }
  if (OB_NOT_NULL(read_buf)) {
    ob_free(read_buf);
    read_buf = nullptr;
  }
  return ret;
}

int ObSharedLogUtilsImpl::get_storage_dest_and_id_(
  share::ObBackupDest &dest,
  uint64_t &storage_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, device_config) {
    if (OB_FAIL(ObDeviceConfigMgr::get_instance().get_device_config(
      ObStorageUsedType::TYPE::USED_TYPE_LOG, device_config))) {
        CLOG_LOG(WARN, "get_device_config failed", K(device_config));
      } else if (OB_FAIL(dest.set(device_config.path_, device_config.endpoint_, device_config.access_info_,
                                  device_config.extension_))) {
      CLOG_LOG(WARN, "set ObBackupDest failed", K(device_config));
    } else {
      storage_id = device_config.storage_id_;
    }
  }
  return ret;
}

int ObSharedLogUtilsImpl::delete_blocks_impl_(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &start_block_id,
  const int64_t delete_count)
{
  int ret = OB_SUCCESS;
  ObBackupDest dest;
  ObTimeGuard time_guard("delete_blocks", 1 * 1000 * 1000);
  uint64_t storage_id = UINT64_MAX;
  if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
    CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id));
  } else {
    char uri_ptr[OB_MAX_URI_LENGTH] = {'\0'};
    ObBackupIoAdapter adapter;
    for (int64_t index = 0; index < delete_count && OB_SUCC(ret); index++) {
      const block_id_t curr_block_id = start_block_id + index;
      if (OB_FAIL(construct_block_str_(dest, tenant_id, ls_id, curr_block_id, uri_ptr, sizeof(uri_ptr)))) {
        CLOG_LOG(WARN, "construct_block_str_ failed", K(uri_ptr));
      } else if (OB_FAIL(adapter.del_file(uri_ptr, dest.get_storage_info()))) {
        CLOG_LOG(WARN, "del_file failed", K(uri_ptr));
        ret = convert_ret_code_(ret);
      }
    }
    CLOG_LOG(TRACE, "delete_blocks_impl_ success", K(delete_count), K(time_guard));
  }
  return ret;
}

int ObSharedLogUtilsImpl::convert_last_non_zero_char_to_zero_(char *uri)
{
  int ret = OB_SUCCESS;
  int64_t uri_len = (NULL == uri ? -1 : strlen(uri));
  if (0 >= uri_len) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected error, uri_len is zero", KP(uri), K(uri_len));
  } else {
    uri[uri_len - 1] = '\0';
  }
  return ret;
}

int ObSharedLogUtilsImpl::convert_ret_code_(const int ret_code)
{
  int ret = ret_code;
  switch (ret_code) {
    case OB_OBJECT_NOT_EXIST:
    case OB_DIR_NOT_EXIST:
      ret = OB_ENTRY_NOT_EXIST;
      break;
    default:
      break;
  }
  return ret;
}

int ObSharedLogUtilsImpl::binary_search_block_by_scn_(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const share::SCN &target_scn,
  const palf::block_id_t start_block_id,
  const palf::block_id_t end_block_id,
  palf::block_id_t &out_block_id,
  share::SCN &out_block_min_scn) 
{
  int ret = OB_SUCCESS;
  block_id_t left_block_id = start_block_id;
  block_id_t right_block_id = end_block_id - 1;
  block_id_t mid_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  out_block_id = LOG_INVALID_BLOCK_ID;
  out_block_min_scn.set_invalid();
  SCN mid_scn;
  int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(get_oldest_block(tenant_id, ls_id, min_block_id))) {
    CLOG_LOG(WARN, "get_oldest_block failed", K(ret), K(tenant_id), K(ls_id), K(min_block_id));
  } else {
    int64_t binary_search_count = 0;
    common::ObBitSet<> non_existed_block_set;
    // check if need to relocate left_block_id
    left_block_id = (min_block_id > left_block_id) ? min_block_id : left_block_id;
    if (is_valid_block_id(min_block_id) && min_block_id > right_block_id) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    
    while (OB_SUCC(ret) && is_valid_block_id(left_block_id) 
                        && is_valid_block_id(right_block_id) 
                        && left_block_id <= right_block_id) {
      mid_block_id = left_block_id + ((right_block_id - left_block_id) >> 1);
      bool mid_existed = false;
      if (OB_FAIL(is_block_existed_(tenant_id, ls_id, mid_block_id,
                                    mid_existed, non_existed_block_set))) {
        CLOG_LOG(WARN, "is_block_existed failed", K(ret), K(tenant_id), K(ls_id), K(mid_block_id), K(mid_existed));
      } else if (!mid_existed && OB_FAIL(relocate_mid_block_(tenant_id, ls_id, left_block_id, right_block_id, 
                                                             mid_block_id, non_existed_block_set))) {
        // binary_search_count != 0 means that it found a mid_block in a previous search. 
        // But it's scn is greater than target scn, so ret should be set OB_ERR_OUT_OF_LOWER_BOUND
        if (OB_ENTRY_NOT_EXIST == ret && 0 != binary_search_count) {
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
          CLOG_LOG(INFO, "scns of all blocks within the range are bigger than target_scn, set ret",
                   K(left_block_id), K(right_block_id), K(mid_block_id), K(binary_search_count), K(out_block_id));
        }
        CLOG_LOG(WARN, "relocate_mid_block_ failed", K(ret), K(left_block_id), K(right_block_id), 
                 K(mid_block_id), K(binary_search_count));
      } else if (OB_FAIL(get_block_min_scn(tenant_id, ls_id, mid_block_id, mid_scn))) {
          // this means the mid_block was deleted just in time, keep searching
          if (OB_ENTRY_NOT_EXIST != ret) {
            CLOG_LOG(WARN, "get_block_min_scn failed", K(ret), K(tenant_id), K(ls_id), K(mid_block_id), K(binary_search_count));
          } else if (OB_FAIL(non_existed_block_set.add_member(mid_block_id))) {
            CLOG_LOG(WARN, "fail to add member into bitset", K(ret), K(mid_block_id));
          } else {
            ret = OB_SUCCESS;
          }
      } else {
        if (mid_scn <= target_scn) {
          // target block must exist in range [mid_block_id, right_block_id]
          out_block_id = mid_block_id;
          out_block_min_scn = mid_scn;
          if (mid_scn == target_scn) {
            break;
          } else {
            left_block_id = mid_block_id + 1;
          }
        } else if (mid_scn > target_scn) {
          // target block may exist in range [left_block_id, mid_block_id - 1]
          if (left_block_id == mid_block_id && LOG_INVALID_BLOCK_ID == out_block_id) {
            // it means that any scn in blocks is bigger than the argument scn.
            ret = OB_ERR_OUT_OF_LOWER_BOUND;
            CLOG_LOG(WARN, "scns of all blocks are bigger than target_scn", K(tenant_id), K(ls_id), K(mid_block_id), K(target_scn));
          } else {
            right_block_id = mid_block_id - 1;
          }
        }
        binary_search_count++;
      }
    }
    if (LOG_INVALID_BLOCK_ID != out_block_id) {
      ret = OB_SUCCESS;
    }
    int64_t used_time = ObTimeUtility::current_time() - start_ts;
    CLOG_LOG(TRACE, "binary_search_block_by_scn_ execution completes", K(ret), K(target_scn), K(start_block_id), 
             K(end_block_id), K(out_block_id), K(out_block_min_scn), K(binary_search_count), K(used_time));
  }
  return ret;
}

int ObSharedLogUtilsImpl::relocate_mid_block_(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  palf::block_id_t &start_block_id,
  palf::block_id_t &end_block_id,
  palf::block_id_t &existed_block_id,
  common::ObBitSet<> &non_existed_block_set)
{
  int ret = OB_SUCCESS;
  bool mid_existed = false;
  // traverse to end_block_id to find an existed block at first
  block_id_t curr_block_id = existed_block_id + 1;
  while (OB_SUCC(ret) && !mid_existed && curr_block_id <= end_block_id) {
    if (OB_FAIL(is_block_existed_(tenant_id, ls_id, curr_block_id, mid_existed, non_existed_block_set))) {
      CLOG_LOG(WARN, "is_block_existed failed", K(ret), K(tenant_id), K(ls_id), K(curr_block_id), K(mid_existed));
    } else if (mid_existed) {
      existed_block_id = curr_block_id;
    } else if (OB_FAIL(non_existed_block_set.add_member(curr_block_id))){
      CLOG_LOG(WARN, "fail to add member into bitset", K(ret), K(curr_block_id));
    } else {
      curr_block_id++;
    }
  }
  // if last traverse not find an existed block, traverse in the other direction
  curr_block_id = mid_existed ? curr_block_id : existed_block_id - 1;
  // it also means no block exists in [existed_block_id, end_block_id]
  end_block_id = mid_existed ? end_block_id : existed_block_id - 1;
  while (OB_SUCC(ret) && !mid_existed && curr_block_id >= start_block_id) {
    if (OB_FAIL(is_block_existed_(tenant_id, ls_id, curr_block_id, mid_existed, non_existed_block_set))) {
      CLOG_LOG(WARN, "is_block_existed failed", K(ret), K(tenant_id), K(ls_id), K(curr_block_id), K(mid_existed));
    } else if (mid_existed) {
      existed_block_id = curr_block_id;
    } else if (OB_FAIL(non_existed_block_set.add_member(curr_block_id))){
      CLOG_LOG(WARN, "fail to add member into bitset", K(ret), K(curr_block_id));
    } else {
      curr_block_id--;
    }
  }
  if (OB_SUCCESS == ret && !mid_existed) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  CLOG_LOG(TRACE, "relocate_mid_block completes", K(start_block_id), K(end_block_id), K(mid_existed), K(existed_block_id));
  
  return ret;
}

int ObSharedLogUtilsImpl::is_block_existed_(
  const uint64_t tenant_id,
  const share::ObLSID &ls_id,
  const palf::block_id_t &block_id,
  bool &existed,
  common::ObBitSet<> &non_existed_block_set)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObBackupDest dest;
  if (non_existed_block_set.has_member(block_id)) {
    existed = false;
  } else {
    char uri[OB_MAX_URI_LENGTH] = {'\0'};
    char *read_buf =
        reinterpret_cast<char *>(ob_malloc(MAX_INFO_BLOCK_SIZE, "ObLogExtU"));
    int64_t read_size = 0;
    int64_t pos = 0;
    uint64_t storage_id = UINT64_MAX;
    if (OB_ISNULL(read_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "allocate read buf failed", K(tenant_id), K(ls_id), K(block_id));
    } else if (OB_FAIL(get_storage_dest_and_id_(dest, storage_id))) {
      CLOG_LOG(WARN, "get_storage_dest_and_id_ failed", K(tenant_id), K(ls_id), K(block_id));
    } else if (OB_FAIL(construct_block_str_(dest, tenant_id, ls_id, block_id, uri, sizeof(uri)))) {
      CLOG_LOG(WARN, "construct_block_str_ failed", K(dest), K(tenant_id), K(ls_id), K(block_id));
    } else if (OB_FAIL(adapter.is_exist(uri, dest.get_storage_info(), existed))) {
      ret = convert_ret_code_(ret);
      CLOG_LOG(WARN, "is_exist failed", K(tenant_id), K(ls_id), K(block_id), K(existed));
    } else if (!existed && OB_FAIL(non_existed_block_set.add_member(block_id)) ) {
        CLOG_LOG(WARN, "fail to add member into bitset", K(block_id));
    } else { 
      CLOG_LOG(TRACE, "is_block_existed_ succeed", K(tenant_id), K(ls_id), K(block_id), K(existed));
    }
    if (OB_NOT_NULL(read_buf)) {
      ob_free(read_buf);
      read_buf = nullptr;
    }
  }

  return ret;
}


// ===================================================================================================
} // end namespace logservice
} // end namespace oceanbase
