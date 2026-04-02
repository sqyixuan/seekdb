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
#include "ob_shared_log_interface.h"
#include "ob_shared_log_utils.h"                    // ObSharedLogUtils
#include "logservice/ob_log_service.h"              // ObLogService
namespace oceanbase
{
using namespace share;
using namespace palf;
namespace logservice
{
const int64_t ObSharedLogInterface::SHARED_LOG_TIMEGUARD_THRESHOLD = 50 * 1000;

ObSharedLogInterface::ObSharedLogInterface()
{}

ObSharedLogInterface::~ObSharedLogInterface()
{
}

int ObSharedLogInterface::locate_by_scn_coarsely(const ObLSID &id,
                                                 const SCN &scn,
                                                 LSN &result_lsn)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("locate_by_scn_coarsely", SHARED_LOG_TIMEGUARD_THRESHOLD);
  LSN begin_lsn;
  if (!id.is_valid() || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id), K(scn));
  } else if (OB_FAIL(locate_by_scn_coarsely_(id, scn, begin_lsn, result_lsn))) {
    CLOG_LOG(WARN, "locate_by_scn_coarsely_ failed", K(id), K(scn));
  } else {
  }
  timeguard.click("after locate_by_scn_coarsely_");
  return ret;
}

int ObSharedLogInterface::locate_by_lsn_coarsely(const ObLSID &id,
                                                 const LSN &lsn,
                                                 SCN &result_scn)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("locate_by_lsn_coarsely",SHARED_LOG_TIMEGUARD_THRESHOLD);
  if (!id.is_valid() || !lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id), K(lsn));
  } else if (OB_FAIL(locate_by_lsn_coarsely_(id, lsn, result_scn))) {
    CLOG_LOG(WARN, "locate_by_lsn_coarsely_ failed", KR(ret), K(id), K(lsn));
  } else {
  }
  timeguard.click("after locate_by_lsn_coarsely_");
  return ret;
}

int ObSharedLogInterface::get_begin_lsn(const ObLSID &ls_id,
                                        LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("get_begin_lsn",SHARED_LOG_TIMEGUARD_THRESHOLD);
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ls_id));
  } else if (OB_FAIL(get_begin_lsn_(ls_id, begin_lsn))) {
    CLOG_LOG(WARN, "get_begin_lsn_ failed", K(ls_id));
  } else {
  }
  timeguard.click("after get_begin_lsn_");
  return ret;
}

int ObSharedLogInterface::get_palf_base_info(const ObLSID &ls_id,
                                             const LSN &base_lsn,
                                             PalfBaseInfo &base_info)
{
  ObTimeGuard timeguard("get_palf_base_info_",SHARED_LOG_TIMEGUARD_THRESHOLD);
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !base_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ls_id), K(base_lsn));
  } else if (OB_FAIL(get_palf_base_info_(ls_id, base_lsn, base_info))) {
    CLOG_LOG(WARN, "get_palf_base_info_ failed", KR(ret), K(ls_id), K(base_lsn));
  } else {
    CLOG_LOG(INFO, "get_palf_base_info_ success", K(ls_id), K(base_lsn), K(base_lsn));
  }
  timeguard.click("after get_palf_base_info_");
  return ret;
}

int ObSharedLogInterface::allocate_hybrid_storage_(const ObLSID &id,
                                                   const LSN &start_lsn,
                                                   const int64_t default_suggested_read_buf_size,
                                                   ObLogHybridStorage *&hybrid_storage)
{
  int ret = OB_SUCCESS;
  hybrid_storage = NULL;
  char *ptr = NULL;
  uint64_t tenant_id = MTL_ID();
  ObLogService *logservice = MTL(ObLogService*);
  ObLogExternalStorageHandler *log_ext_handler = NULL;
  if (OB_ISNULL(logservice)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error, logservice is nullptr", KP(logservice), K(id), K(tenant_id), K(start_lsn));
  } else if (FALSE_IT(log_ext_handler = logservice->get_log_ext_handler())) {
  } else if (OB_ISNULL(log_ext_handler)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "unexpected error, log_ext_handler is nullptr", KP(logservice), K(id), K(tenant_id), K(start_lsn));
  } else if (FALSE_IT(ptr = reinterpret_cast<char *>(mtl_malloc(sizeof(ObLogHybridStorage), "ObLogHybridS")))) {
  } else if (OB_ISNULL(ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate memory failed", K(id), K(start_lsn));
  } else if (FALSE_IT(hybrid_storage = new(ptr)ObLogHybridStorage())) {
  } else if (OB_FAIL(hybrid_storage->init(tenant_id, id.id(), start_lsn, default_suggested_read_buf_size, log_ext_handler))) {
    CLOG_LOG(WARN, "init hybrid_storage_ failed", K(id), K(start_lsn), KP(hybrid_storage));
  } else {
    CLOG_LOG(TRACE, "allocate_hybrid_storage_ success", K(id), K(start_lsn), KP(hybrid_storage));
  }

  if (OB_FAIL(ret)) {
    if (hybrid_storage != NULL) {
      hybrid_storage->~ObLogHybridStorage();
    }
    if (NULL != ptr) {
      mtl_free(ptr);
      ptr = NULL;
    }
    hybrid_storage = NULL;
  }
  return ret;
}

void ObSharedLogInterface::free_hybrid_storage_(ObLogHybridStorage *&hybrid_storage)
{
  if (NULL != hybrid_storage) {
    hybrid_storage->~ObLogHybridStorage();
    mtl_free(hybrid_storage);
    hybrid_storage = NULL;
  }
}

int ObSharedLogInterface::locate_by_scn_coarsely_(const ObLSID &id,
                                                  const SCN &scn,
                                                  LSN &begin_lsn,
                                                  LSN &result_lsn)
{
  int ret = OB_SUCCESS;
  bool need_locate_on_shared_log = false;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t base_block_id = LOG_INITIAL_BLOCK_ID;
  const uint64_t tenant_id = MTL_ID();
  PalfHandleGuard guard;
  const bool support_shared_log = GCTX.is_shared_storage_mode();
  if (OB_FAIL(open_palf_(id, guard)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "open_palf_failed");
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // palf has been deleted, try get min/max block id from shared log
    if (support_shared_log
        && OB_SUCC(get_block_info_of_shared_log_(tenant_id, id, base_block_id, min_block_id, max_block_id))) {
      need_locate_on_shared_log = true;
    }
    // locate scn on local storage firstly.
    // OB_ERR_OUT_OF_LOWER_BOUND means that the scn is smaller than the local min scn
  } else if (OB_FAIL(guard.locate_by_scn_coarsely(scn, result_lsn))
             && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    CLOG_LOG(WARN, "locate_by_scn_coarsely on local failed", K(id), K(scn));
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    LSN base_lsn;
    base_block_id = LOG_INVALID_LSN_VAL;
    if (OB_FAIL(guard.get_palf_handle()->get_begin_lsn(begin_lsn))) {
      CLOG_LOG(WARN, "get_begin_lsn failed", K(id), K(scn));
    } else if (OB_FAIL(guard.get_palf_handle()->get_base_lsn(base_lsn))) {
      CLOG_LOG(WARN, "get_base_lsn failed", K(id), K(scn));
    } else if (FALSE_IT(base_block_id = lsn_2_block(base_lsn, PALF_BLOCK_SIZE))) {
    } else if (LOG_INITIAL_BLOCK_ID != base_block_id && FALSE_IT(base_block_id -= 1)) {
    } else if (support_shared_log) {
      if (OB_SUCC(get_block_info_of_shared_log_(tenant_id, id, base_block_id, min_block_id, max_block_id))) {
        need_locate_on_shared_log = true;
      // Assume that blocks on shared storage and local storage are contigtuous, when there is
      // no such block on shared storage, convert OB_ENTRY_NOT_EXIST to OB_ERR_OUT_OF_LOWER_BOUND.
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_ERR_OUT_OF_LOWER_BOUND;
      }
    }
  } else {
  }
  if (OB_SUCC(ret) && need_locate_on_shared_log) {
    ret = locate_by_scn_on_shared_storage_(tenant_id, id, min_block_id, max_block_id, scn, result_lsn);
    begin_lsn = LSN(min_block_id * PALF_BLOCK_SIZE);
  }
  return ret;
}

int ObSharedLogInterface::locate_by_scn_on_shared_storage_(const uint64_t tenant_id,
                                                           const ObLSID &id,
                                                           const block_id_t &min_block_id,
                                                           const block_id_t &max_block_id,
                                                           const SCN &scn,
                                                           LSN &result_lsn)
{
  int ret = OB_SUCCESS;
  SCN result_scn;
  block_id_t result_block_id = LOG_INVALID_BLOCK_ID;
  if (OB_FAIL(ObSharedLogUtils::locate_by_scn_coarsely(tenant_id, id, min_block_id, max_block_id + 1,
                                                       scn, result_block_id, result_scn))) {
    CLOG_LOG(WARN, "locate_by_scn_coarsely failed", K(tenant_id), K(id), K(min_block_id), K(max_block_id), K(scn));
  } else {
    result_lsn.val_ = result_block_id * PALF_BLOCK_SIZE;
    CLOG_LOG(INFO, "locate_by_scn_on_shared_storage_ success", K(tenant_id), K(id), K(min_block_id),
             K(max_block_id), K(scn), K(result_lsn),K(result_scn));
  }
  return ret;
}

int ObSharedLogInterface::locate_by_lsn_coarsely_(const ObLSID &id,
                                                  const LSN &lsn,
                                                  SCN &result_scn)
{
  int ret = OB_SUCCESS;
  block_id_t block_id = lsn_2_block(lsn, PALF_BLOCK_SIZE);
  const uint64_t tenant_id = MTL_ID();
  PalfHandleGuard guard;
  bool need_locate_on_shared_log = false;
  const bool support_shared_log = GCTX.is_shared_storage_mode();
  if (OB_FAIL(open_palf_(id, guard)) && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "open_palf_failed", K(id), K(lsn));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    // palf has been deleted, if GCTX.is_shared_storage_mode() is true, locate it on shared log.
    if (support_shared_log) {
      need_locate_on_shared_log = true;
    }
  } else if (OB_FAIL(guard.locate_by_lsn_coarsely(lsn, result_scn))
             && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    CLOG_LOG(WARN, "locate_by_lsn_coarsely", KR(ret), K(id), K(lsn));
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
    // if GCTX.is_shared_storage_mode() is true, locate it on shared log.
    if (support_shared_log) {
      need_locate_on_shared_log = true;
      ret = OB_SUCCESS;
    }
  } else {
  }
  if (OB_SUCC(ret) && need_locate_on_shared_log) {
    ret = ObSharedLogUtils::get_block_min_scn(tenant_id, id, block_id, result_scn);
    // Assume that blocks on shared storage and local storage are contigtuous, when there is
    // no such block on shared storage, convert OB_ENTRY_NOT_EXIST to OB_ERR_OUT_OF_LOWER_BOUND.
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
    }
  }
  return ret;
}

int ObSharedLogInterface::get_block_info_of_shared_log_(const uint64_t tenant_id,
                                                        const ObLSID &id,
                                                        const block_id_t &base_block_id,
                                                        block_id_t &min_block_id,
                                                        block_id_t &max_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSharedLogUtils::get_oldest_block(tenant_id, id, min_block_id))) {
    CLOG_LOG(WARN, "get_oldest_block failed", K(tenant_id), K(id));
  } else if (OB_FAIL(ObSharedLogUtils::get_newest_block(tenant_id, id, base_block_id, max_block_id))) {
    CLOG_LOG(WARN, "get_newest_block failed", K(tenant_id), K(id), K(min_block_id), K(base_block_id));
  } else {
  }
  return ret;
}

int ObSharedLogInterface::open_palf_(const ObLSID &id,
                                     PalfHandleGuard &palf_handle_guard)
{
  int ret = OB_SUCCESS;
  ObLogService *logservice = MTL(ObLogService*);
  if (OB_ISNULL(logservice)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(logservice->open_palf(id, palf_handle_guard))) {
    CLOG_LOG(WARN, "open_palf failed", K(id));
  } else {
  }
  return ret;
}

int ObSharedLogInterface::get_begin_lsn_(const ObLSID &ls_id,
                                         LSN &begin_lsn)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  block_id_t oldest_block = LOG_INVALID_BLOCK_ID;
  if (OB_FAIL(ObSharedLogUtils::get_oldest_block(tenant_id, ls_id, oldest_block))) {
    CLOG_LOG(WARN, "there is no log blocks on shared storage", KR(ret), K(ls_id));
  } else {
    begin_lsn = LSN(oldest_block * PALF_BLOCK_SIZE);
  }
  return ret;
}

int ObSharedLogInterface::get_palf_base_info_(const ObLSID &ls_id,
                                              const LSN &base_lsn,
                                              PalfBaseInfo &base_info)
{
  int ret = OB_SUCCESS;
  const LSN initial_lsn(PALF_INITIAL_LSN_VAL);
  const uint64_t tenant_id = MTL_ID();
  base_info.generate_by_default();
  base_info.curr_lsn_ = base_lsn;
  LogInfo &prev_log_info = base_info.prev_log_info_;
  // NB: when base_lsn.val_ is not PALF_INITIAL_LSN_VAL, need iterate prev block
  // there is at least one clog object on object storage, otherwise, we can not construct PalfBaseInfo.
  block_id_t lsn_block_id = lsn_2_block(base_lsn, PALF_BLOCK_SIZE);
  offset_t lsn_block_offset = lsn_2_offset(base_lsn, PALF_BLOCK_SIZE);
  LSN start_lsn;
  if (LOG_INITIAL_BLOCK_ID == lsn_block_id) {
    start_lsn.val_ = lsn_block_id * PALF_BLOCK_SIZE;
  } else {
    start_lsn.val_ =
      (0ul == lsn_block_offset ? (lsn_block_id-1) * PALF_BLOCK_SIZE : lsn_block_id * PALF_BLOCK_SIZE);
  }
  const int64_t default_suggested_read_buf_size = PALF_BLOCK_SIZE;
  PalfIterator<LogGroupEntry> iterator;
  if (base_lsn == initial_lsn) {
    ret = OB_SUCCESS;
    base_info.generate_by_default();
  } else if (OB_FAIL(ObSharedLogInterface::seek(ls_id, start_lsn, default_suggested_read_buf_size, iterator))) {
    CLOG_LOG(WARN, "get_prev_log_info_ failed", KR(ret), K(ls_id), K(base_lsn));
  } else if (OB_FAIL(iterator.set_io_context(LogIOContext(tenant_id, ls_id.id(), LogIOUser::META_INFO)))) {
    CLOG_LOG(WARN, "set_io_context failed", KR(ret), K(ls_id), K(base_lsn));
  } else {
    LogGroupEntryHeader prev_entry_header;
    LogGroupEntry curr_entry;
    LSN prev_lsn;
    LSN curr_lsn;
    while (OB_SUCC(iterator.next())) {
      if (OB_FAIL(iterator.get_entry(curr_entry, curr_lsn))) {
        CLOG_LOG(WARN, "get_entry failed", KR(ret), K(curr_lsn));
      } else if (curr_lsn + curr_entry.get_serialize_size() > base_lsn) {
        ret = OB_ITER_END;
        break;
      } else {
        prev_entry_header = curr_entry.get_header();
        prev_lsn = curr_lsn;
      }
    }
    if (OB_ITER_END == ret) {
      if (!prev_lsn.is_valid()) {
        if (curr_lsn <= base_lsn) {
          ret = OB_ERR_OUT_OF_LOWER_BOUND;
          CLOG_LOG(WARN, "get_palf_base_info_ out of lower bound", KR(ret), K(ls_id), K(curr_lsn), K(base_lsn));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          PALF_LOG(WARN, "there is no log before base_lsn", KR(ret), K(base_lsn), K(start_lsn));
        }
      } else if (prev_lsn >= base_lsn) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "prev lsn must be smaller than base_lsn", K(ret), K(base_lsn), K(prev_lsn), K(prev_entry_header));
      } else {
        prev_log_info.log_id_ = prev_entry_header.get_log_id();
        prev_log_info.scn_ = prev_entry_header.get_max_scn();
        prev_log_info.accum_checksum_ = prev_entry_header.get_accum_checksum();
        prev_log_info.log_proposal_id_ = prev_entry_header.get_log_proposal_id();
        prev_log_info.lsn_ = prev_lsn;
        ret = OB_SUCCESS;
      }
    }
    if (OB_SUCC(ret)) {
      CLOG_LOG(INFO, "get_palf_base_info_ success",K(ls_id), K(base_lsn), K(base_info));
    }
  }
  return ret;
}
}//end of namespace logservice
}//end of namespace oceanbase
