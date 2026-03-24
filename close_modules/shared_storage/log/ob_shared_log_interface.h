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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_SHARED_LOG_INTERFACE_
#define OCEANBASE_LOGSERVICE_OB_LOG_SHARED_LOG_INTERFACE_

#include "ob_log_iterator_storage.h"

namespace oceanbase
{
namespace logservice
{
class ObLogExternalStorageHandler;
class ObSharedLogInterface
{
public:
  ObSharedLogInterface();
  ~ObSharedLogInterface();

public:
  // ========================== Shared log start =============================
  // @desc: seek a palf iterator by lsn, the first log A in iterator must meet
  //        the start lsn of log A must equal to 'start_lsn'.
  // @params [in] id:
  // @params [in] start_lsn:
  // @params [in] suggested_max_read_buffer_size:
  // @params [out] iter: buffer iterator in which all logs's lsn are higher to 'start_lsn'
  //                    (include 'start_lsn').
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT
  // - OB_ALLOCATE_MEMORY_FAILED
  // - OB_ENTRY_NOT_EXIST: there is no log's lsn is higher than lsn
  // - OB_ERR_OUT_OF_LOWER_BOUND: lsn is too small, log files may have been recycled
  // - others: bug
  template <typename LogEntryType>
  static int seek(const share::ObLSID &id,
                  const palf::LSN &start_lsn,
                  const int64_t suggested_max_read_buffer_size,
                  palf::PalfIterator<LogEntryType> &iterator);

  // @desc: seek a palf iterator by scn, the first A in iterator must meet
  // one of the following conditions:
  // 1. scn of log A equals to scn
  // 2. scn of log A is higher than scn and A is the first log which scn is higher
  // than scn in all committed logs
  // Note that this function may be time-consuming
  // @params [in] id:
  // @params [in] scn:
  // @params [in] suggested_max_read_buffer_size:
  // @params [out] group_iterator: log group buffer iterator in which all logs's scn are higher than/equal to scn
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT
  // - OB_ALLOCATE_MEMORY_FAILED
  // - OB_ENTRY_NOT_EXIST: there is no log's scn is higher than scn
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too old, log files may have been recycled
  // - others: bug
  template <typename LogEntryType>
  static int seek(const share::ObLSID &id,
                  const share::SCN &scn,
                  const int64_t suggested_max_read_buffer_size,
                  palf::PalfIterator<LogEntryType>& iterator);

  // @desc: query coarse lsn by scn, that means there is a LogGroupEntry in disk,
  //        its lsn and scn are result_lsn and result_scn, and result_scn <= scn.
  // Note that this function may be time-consuming
  // Note that result_lsn always points to head of log file
  // @params [in] id:
  // @params [in] scn:
  // @params [out] result_lsn: the lower bound lsn which includes scn
  // @return
  // - OB_SUCCESS: locate_by_scn_coarsely success
  // - OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED.
  // - OB_ENTRY_NOT_EXIST: there is no log's scn is higher than scn
  // - OB_ERR_OUT_OF_LOWER_BOUND: scn is too small, log files may have been recycled
  // - others: bug
  static int locate_by_scn_coarsely(const share::ObLSID &id,
                                    const share::SCN &scn,
                                    palf::LSN &result_lsn);
  // @desc: query coarse scn by lsn, that means there is a LogGroupEntry in disk,
  //        its lsn and scn are result_lsn and result_scn, and result_lsn <= lsn.
  // Note that this function may be time-consuming
  // Note that result_scn always points to head of log file
  // @params [in] id:
  // @params [in] lsn:
  // @params [out] result_scn: the lower bound snc which includes lsn 
  // @return
  // - OB_SUCCESS: locate_by_lsn_coarsely success
  // - OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED.
  // - OB_ENTRY_NOT_EXIST: there is no log's lsn is higher than lsn
  // - OB_ERR_OUT_OF_LOWER_BOUND: lsn is too small, log files may have been recycled
  // - others: bug
  static int locate_by_lsn_coarsely(const share::ObLSID &id,
                                    const palf::LSN &lsn,
                                    share::SCN &result_scn);
  static int get_begin_lsn(const ObLSID &ls_id,
                           palf::LSN &begin_lsn);

  // @desc: get palf base info by lsn.
  // Note that this function may be time-consuming
  // @params [in] ls_id:
  // @params [base_lsn] base_lsn:
  // @params [out] palf_base_info:
  // @return
  // - OB_SUCCESS
  // - OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED
  // - OB_ENTRY_NOT_EXIST: there is no log whose lsn is less than base_lsn
  // - OB_ERR_OUT_OF_LOWER_BOUND: base_lsn is too small, log files may have been recycled
  // - others: bug
  static int get_palf_base_info(const ObLSID &ls_id,
                                const palf::LSN &base_lsn,
                                palf::PalfBaseInfo &palf_base_info);
  // ========================== Shared log end =============================

private:
  // ========================== Shared log start =============================
  static const int64_t SHARED_LOG_TIMEGUARD_THRESHOLD;
  static int allocate_hybrid_storage_(const share::ObLSID &id,
                                      const palf::LSN &start_lsn,
                                      const int64_t suggested_max_read_buffer_size,
                                      ObLogHybridStorage *&hybrid_storage);
  static void free_hybrid_storage_(ObLogHybridStorage *&hybrid_storage);
  static int locate_by_scn_coarsely_(const share::ObLSID &id,
                                     const share::SCN &scn,
                                     palf::LSN &begin_lsn,
                                     palf::LSN &result_lsn);
  static int locate_by_scn_on_shared_storage_(const uint64_t tenant_id,
                                              const share::ObLSID &id,
                                              const palf::block_id_t &min_block_id,
                                              const palf::block_id_t &max_block_id,
                                              const share::SCN &scn,
                                              palf::LSN &result_lsn);
  static int locate_by_lsn_coarsely_(const share::ObLSID &id,
                                     const palf::LSN &lsn,
                                     SCN &result_scn);
  static int get_block_info_of_shared_log_(const uint64_t tenant_id,
                                           const ObLSID &id,
                                           const palf::block_id_t &base_block_id,
                                           palf::block_id_t &min_block_id,
                                           palf::block_id_t &max_block_id);
  static int open_palf_(const share::ObLSID &id,
                        palf::PalfHandleGuard &palf_handle);
  template <typename LogEntryType>
  static int seek_iterator_from_lsn_impl_(const share::ObLSID &id,
                                          const palf::LSN &lsn,
                                          const int64_t suggested_max_read_buffer_size,
                                          palf::PalfIterator<LogEntryType> &iterator);
  template <typename LogEntryType>
  static int seek_iterator_from_scn_impl_(const share::ObLSID &id,
                                          const share::SCN &scn,
                                          const int64_t suggested_max_read_buffer_size,
                                          palf::PalfIterator<LogEntryType> &iterator);
  template <typename LogEntryType>
  static int find_first_log_after_scn_(const ObLSID &ls_id,
                                       const palf::LSN &start_lsn,
                                       const SCN &scn,
                                       palf::LSN &result_lsn);
  static int get_begin_lsn_(const ObLSID &ls_id,
                            palf::LSN &begin_lsn);
  static int get_palf_base_info_(const ObLSID &ls_id,
                                 const palf::LSN &base_lsn,
                                 palf::PalfBaseInfo &palf_base_info);
  // ========================== Shared log end =============================
private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedLogInterface);
};

struct SharedLogDestroyIteratorStorageFunctor
{
  SharedLogDestroyIteratorStorageFunctor(ObLogHybridStorage *log_storage)
    : log_storage_(log_storage) {}
  ~SharedLogDestroyIteratorStorageFunctor()
  {
  }
  SharedLogDestroyIteratorStorageFunctor(const SharedLogDestroyIteratorStorageFunctor &rhs)
  {
    operator=(rhs);
  }
  SharedLogDestroyIteratorStorageFunctor(SharedLogDestroyIteratorStorageFunctor &&rhs) = delete;
  SharedLogDestroyIteratorStorageFunctor& operator=(const SharedLogDestroyIteratorStorageFunctor &rhs) 
  {
    if (*this == rhs) {
      return *this;
    }
    log_storage_ = rhs.log_storage_;
    return *this;
  }
  SharedLogDestroyIteratorStorageFunctor& operator=(SharedLogDestroyIteratorStorageFunctor &&rhs) = delete;
  bool operator==(const SharedLogDestroyIteratorStorageFunctor &rhs) const
  {
    return this->log_storage_ == rhs.log_storage_;
  }
  void operator()()
  {
    if (NULL != log_storage_) {
      log_storage_->~ObLogHybridStorage();
      mtl_free(log_storage_);
      log_storage_ = NULL;
    }
  }
  ObLogHybridStorage *log_storage_;
};

template <typename LogEntryType>
int ObSharedLogInterface::seek_iterator_from_lsn_impl_(const share::ObLSID &id,
                                                       const palf::LSN &start_lsn,
                                                       const int64_t suggested_max_read_buffer_size,
                                                       palf::PalfIterator<LogEntryType> &iterator)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("seek_iterator_from_lsn",SHARED_LOG_TIMEGUARD_THRESHOLD);
  ObLogHybridStorage *hybrid_storage = NULL;
  palf::GetFileEndLSN get_file_end_lsn;
  palf::GetModeVersion get_mode_version;
  bool need_release_hybrid_storage = true;
  bool first_init = !iterator.is_inited();
  if (!id.is_valid() || !start_lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id), K(start_lsn));
  } else if (true == iterator.is_inited()) {
    CLOG_LOG(TRACE, "reuse iterator success", K(id), K(start_lsn), K(iterator));
    ret = iterator.reuse(start_lsn);
  } else if (OB_FAIL(allocate_hybrid_storage_(id, start_lsn, suggested_max_read_buffer_size, hybrid_storage))) {
  } else if (FALSE_IT(timeguard.click("after allocate"))) {
    CLOG_LOG(WARN, "allocate_hybrid_storage_ failed", K(id), K(start_lsn));
  } else if (OB_FAIL(hybrid_storage->get_file_end_lsn(get_file_end_lsn))) {
    CLOG_LOG(WARN, "get_file_end_lsn failed", K(id), K(start_lsn));
  } else if (OB_FAIL(hybrid_storage->get_access_mode_version(get_mode_version))) {
    CLOG_LOG(WARN, "get_access_mode_version failed", K(id), K(start_lsn));
  } else if (OB_FAIL(iterator.init(start_lsn, get_file_end_lsn, get_mode_version,
                                   hybrid_storage))) {
    CLOG_LOG(WARN, "init iterator failed", K(id), K(start_lsn));
  } else {
    // NB: the ownership of hybrid_storage has transfered to iterator after set_destroy_iterator_storage_functor successfully,
    //     set_destroy_iterator_storage_functor is atomic(i.e. return OB_SUCCESS means transfer ownership successfully,
    //     otherwise, the ownership hasn't transfered.).
    // To make code readable, add 'need_release_hybrid_storage' instead of using std::move and move constructor
    SharedLogDestroyIteratorStorageFunctor functor(hybrid_storage);
    if (OB_FAIL(iterator.set_destroy_iterator_storage_functor(functor))) {
      CLOG_LOG(WARN, "set_destroy_iterator_storage_functor failed", KR(ret));
    } else {
      need_release_hybrid_storage = false;
    }
  }
  timeguard.click("after init iterator");
  if (need_release_hybrid_storage && NULL != hybrid_storage) {
    CLOG_LOG(WARN, "release hybrid_storage success", KR(ret), KP(hybrid_storage));
    free_hybrid_storage_(hybrid_storage);
  }
  return ret;
}

template <typename LogEntryType>
int ObSharedLogInterface::seek(
  const share::ObLSID &id,
  const palf::LSN &start_lsn,
  const int64_t suggested_max_read_buffer_size,
  palf::PalfIterator<LogEntryType> &iterator)
{
  return seek_iterator_from_lsn_impl_(id, start_lsn, suggested_max_read_buffer_size, iterator);
}

template <typename LogEntryType>
int ObSharedLogInterface::seek(
  const share::ObLSID &id,
  const SCN &scn,
  const int64_t suggested_max_read_buffer_size,
  palf::PalfIterator<LogEntryType> &iterator)
{
  return seek_iterator_from_scn_impl_(id, scn, suggested_max_read_buffer_size, iterator);
}

template <typename LogEntryType>
int ObSharedLogInterface::seek_iterator_from_scn_impl_(
  const share::ObLSID &id,
  const share::SCN &scn,
  const int64_t suggested_max_read_buffer_size,
  palf::PalfIterator<LogEntryType> &iterator)
{
  int ret = OB_SUCCESS;
 palf::LSN start_lsn;
 palf::LSN begin_lsn;
 palf::LSN result_lsn;
  ObTimeGuard timeguard("seek_iterator_from_scn",SHARED_LOG_TIMEGUARD_THRESHOLD);
  if (!id.is_valid() || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(id), K(scn));
  } else if (OB_FAIL(locate_by_scn_coarsely_(id, scn, begin_lsn, start_lsn))
             && OB_ERR_OUT_OF_LOWER_BOUND != ret) {
    CLOG_LOG(WARN, "locate_by_scn_coarsely_ failed", K(id), K(scn));
    // for seek iterator, if there is no log whose scn is smaller than or equal to 'scn',
    // and begin_lsn is PALF_INITIAL_LSN_VAL, should return an iterator whose start_lsn is
    // begin_lsn.
  } else if (FALSE_IT(timeguard.click("after locate"))) {
  } else if (OB_ERR_OUT_OF_LOWER_BOUND == ret
             && !FALSE_IT(start_lsn = begin_lsn)
             && palf::PALF_INITIAL_LSN_VAL != begin_lsn.val_) {
    CLOG_LOG(WARN, "locate_by_lsn_coarsely_ failed", KR(ret), K(start_lsn));
  } else if (OB_FAIL(find_first_log_after_scn_<LogEntryType>(id, start_lsn, scn, result_lsn))) {
    CLOG_LOG(WARN, "find_first_log_after_scn failed", K(id), K(start_lsn), K(scn));
  } else if (FALSE_IT(timeguard.click("after find"))) {
  } else if (OB_FAIL(seek(id, result_lsn, suggested_max_read_buffer_size, iterator))) {
    CLOG_LOG(WARN, "seek failed", K(id), K(scn), K(start_lsn));
  } else if (FALSE_IT(timeguard.click("after seek"))) {
  } else  {
  }
  return ret;
}

template <typename LogEntryType>
int ObSharedLogInterface::find_first_log_after_scn_(const ObLSID &ls_id,
                                                    const palf::LSN &start_lsn,
                                                    const SCN &scn,
                                                    palf::LSN &result_lsn)
{
  int ret = OB_SUCCESS;
  palf::PalfIterator<LogEntryType> iterator;
  const int64_t suggested_max_read_buffer_size = palf::PALF_BLOCK_SIZE;
  if (OB_FAIL(ObSharedLogInterface::seek(ls_id, start_lsn, suggested_max_read_buffer_size, iterator))) {
    CLOG_LOG(WARN, "seek iterator failed", K(ls_id), K(start_lsn), K(scn));
  } else {
    LogEntryType curr_entry;
    palf::LSN curr_lsn;
    while (OB_SUCC(iterator.next())) {
      if (OB_FAIL(iterator.get_entry(curr_entry, curr_lsn))) {
        CLOG_LOG(WARN, "get_entry failed", KR(ret), K(curr_lsn));
      } else if (curr_entry.get_scn() >= scn) {
        result_lsn = curr_lsn;
        break;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      CLOG_LOG(WARN, "there is no log whose scn is greater than or equal to scn", KR(ret), K(ls_id), K(start_lsn),
               K(scn), K(result_lsn));
    }
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
#endif
