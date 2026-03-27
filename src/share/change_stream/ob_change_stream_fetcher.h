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
#include "share/ob_ls_id.h"
#include "share/ob_thread_pool.h"
#include "logservice/palf/lsn.h"
#include "logservice/palf/log_entry.h"
#include "logservice/palf/palf_iterator.h"
#include "storage/tx/ob_tx_seq.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace share
{

/// Interval for advancing min_dep_lsn to global_stat (us).
static constexpr int64_t CS_FETCHER_MIN_DEP_LSN_ADVANCE_INTERVAL_US = 10 * 1000 * 1000;
/// Interval for progress log (us).
static constexpr int64_t CS_FETCHER_PROGRESS_LOG_INTERVAL_US = 10 * 1000 * 1000;
/// Sleep when init checks (sql_proxy not ready, etc.).
static constexpr int64_t CS_FETCHER_INIT_RETRY_SLEEP_US = 500 * 1000;
/// Sleep when init_consumption_position_ fails.
static constexpr int64_t CS_FETCHER_INIT_FAIL_SLEEP_US = 1000 * 1000;
/// Sleep when caught up (OB_ITER_END).
static constexpr int64_t CS_FETCHER_ITER_END_SLEEP_US = 1000 * 1000;
/// Sleep when iter.next/get_entry fails.
static constexpr int64_t CS_FETCHER_ITER_ERR_SLEEP_US = 100 * 1000;
/// tx_info_ hash bucket count.
static constexpr int64_t CS_FETCHER_TX_INFO_BUCKET_CNT = 1024;
/// Max redo mutator size (64MB), sanity check to avoid OOM from corrupted data.
static constexpr int64_t CS_FETCHER_MAX_REDO_MUTATOR_SIZE = 64 * 1024 * 1024;

class ObCSDispatcher;

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
  palf::LSN start_lsn_;           // Used by get_min_dep_lsn for log reclaim and init restart.
  int64_t   schema_version_ = 0;  // Assigned from Fetcher's current_schema_version_ at commit time.
  bool      is_ddl_ = false;      // True if redo contains __all_ddl_operation rows.
  common::ObSEArray<ObCSRollbackRange, 1> rollback_list_;
  common::ObSEArray<ObCSRedoRecord, 1>    redo_list_;

  void reset()
  {
    tx_id_ = 0;
    commit_version_ = 0;
    start_lsn_.reset();
    schema_version_ = 0;
    is_ddl_ = false;
    rollback_list_.reset();
    redo_list_.reset();
  }

  /// Free redo buffers and reset; caller then frees the ObCSTxInfo itself.
  void destroy();

  TO_STRING_KV(K_(tx_id), K_(commit_version), K_(start_lsn), K_(schema_version), K_(is_ddl));
};

// ---------------------------------------------------------------------------
// ObCSFetcher: single-threaded CLOG consumer, assembles by transaction, pushes on commit.
// ---------------------------------------------------------------------------

class ObCSFetcher : public share::ObThreadPool
{
public:
  ObCSFetcher();
  virtual ~ObCSFetcher();

  int init(ObCSDispatcher *dispatcher);
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
  int init_consumption_position_();
  void try_advance_min_dep_lsn_();
  int handle_redo_log_(const transaction::ObTransID &tx_id,
                       const char *mutator_buf,
                       int64_t mutator_size,
                       const palf::LSN &lsn);
  int handle_commit_log_(const transaction::ObTransID &tx_id,
                         const SCN &commit_version);
  int handle_abort_log_(const transaction::ObTransID &tx_id);
  int handle_rollback_to_log_(const transaction::ObTransID &tx_id,
                              const transaction::ObTxSEQ &from,
                              const transaction::ObTxSEQ &to);
  int extract_ddl_schema_version_(ObCSTxInfo *tx, int64_t &schema_version);
  /// Get or create tx in tx_info_; used by handle_redo_log_ and MDS DDL branch.
  int get_or_create_tx_info_(int64_t tid, const palf::LSN &lsn, ObCSTxInfo *&tx);

  bool is_inited_;
  ObCSDispatcher *dispatcher_;
  share::ObLSID ls_id_;
  palf::PalfBufferIterator iter_;
  palf::LSN current_lsn_;
  SCN current_scn_;
  int64_t current_schema_version_;
  common::hash::ObHashMap<int64_t, ObCSTxInfo *> tx_info_; // tx_id -> ObCSTxInfo
  int64_t total_tx_committed_;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OB_CS_FETCHER_H_
