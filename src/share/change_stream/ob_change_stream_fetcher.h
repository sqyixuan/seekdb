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
 *
 * Change Stream Fetcher: data structures for consuming CLOG by transaction.
 * Single-threaded Fetcher maintains ObCSTxInfo per tx_id and pushes committed
 * transactions into the Dispatcher ring buffer.
 */

#ifndef OB_CS_FETCHER_H_
#define OB_CS_FETCHER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_thread_pool.h"
#include "logservice/palf/lsn.h"
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
namespace share
{

/// Buffer for a single redo record (Fetcher maintains these per transaction).
struct ObCSRedoRecord
{
  char   *redo_buf_ = nullptr;
  int64_t redo_len_ = 0;

  void reset()
  {
    redo_buf_ = nullptr;
    redo_len_ = 0;
  }
  TO_STRING_KV(K_(redo_buf), K_(redo_len));
};

/// Rollback range [to_seq, from_seq). Rows with stmt seq_no in this range are not visible.
using ObCSRollbackRange = std::pair<transaction::ObTxSEQ, transaction::ObTxSEQ>;

/// Per-transaction info (Fetcher keyed by tx_id; pushed to Dispatcher ring buffer on commit).
struct ObCSTxInfo
{
  int64_t   tx_id_ = 0;
  int64_t   commit_version_ = 0;   // Used for refresh_scn and filtering.
  palf::LSN start_lsn_;           // Used by get_min_dep_lsn for log reclaim.
  int64_t   schema_version_ = 0;  // Assigned by DDL; which schema version to use for multi-stmt tx.
  common::ObSEArray<ObCSRollbackRange, 1> rollback_list_;
  common::ObSEArray<ObCSRedoRecord, 1>    redo_list_;

  void reset()
  {
    tx_id_ = 0;
    commit_version_ = 0;
    start_lsn_.reset();
    schema_version_ = 0;
    rollback_list_.reset();
    redo_list_.reset();
  }

  /// Free redo buffers and reset; caller then frees the ObCSTxInfo itself.
  void destroy();

  TO_STRING_KV(K_(tx_id), K_(commit_version), K_(start_lsn), K_(schema_version));
};

// ---------------------------------------------------------------------------
// ObCSFetcher: single-threaded CLOG consumer, assembles by transaction, pushes on commit.
// ---------------------------------------------------------------------------

class ObCSFetcher : public share::ObThreadPool
{
public:
  ObCSFetcher();
  virtual ~ObCSFetcher();

  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  /// For log reclaim: returns the minimum LSN still depended on by in-flight tx.
  palf::LSN get_min_dep_lsn() const;

  /// Called by Dispatcher drain loop after Worker commits a batch.
  /// Removes the transaction from tx_info_ map, calls tx->destroy() to free
  /// redo buffers, and frees the ObCSTxInfo object.
  /// Thread-safe: ObHashMap uses bucket-level locking.
  int release_committed_tx(int64_t tx_id);

protected:
  void run1() override;

private:
  bool is_inited_;
  void *log_handler_;                                      // palf::ObLogHandler *, for CLOG read
  palf::LSN current_lsn_;
  SCN current_scn_;
  common::hash::ObHashMap<int64_t, ObCSTxInfo *> tx_info_; // tx_id -> ObCSTxInfo
};

}  // namespace share
}  // namespace oceanbase

#endif  // OB_CS_FETCHER_H_
