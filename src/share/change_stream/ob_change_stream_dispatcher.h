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
 * Change Stream Dispatcher: parsed row, execution context, and subtasks.
 * Single-threaded Dispatcher consumes committed transactions from the ring buffer,
 * parses redo, slices by rowkey hash, and pushes subtasks to Workers.
 */

#ifndef OB_CS_DISPATCHER_H_
#define OB_CS_DISPATCHER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_ext_ring_buffer.h"
#include "common/ob_tablet_id.h"
#include "share/ob_thread_pool.h"
#include "storage/tx/ob_tx_seq.h"
#include "share/change_stream/ob_change_stream_fetcher.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{

class ObCSPlugin;  // Forward declaration; defined in ob_change_stream_plugin.h

// ---------------------------------------------------------------------------
// Dispatcher: one parsed redo row, execution context, subtask
// ---------------------------------------------------------------------------

/// One parsed redo row (Dispatcher outputs these to subtasks by tablet/table).
struct ObCSRow
{
  common::ObTabletID tablet_id_;
  int64_t table_id_ = 0;
  char   *row_buf_ = nullptr;
  int64_t row_len_ = 0;
  TO_STRING_KV(K_(tablet_id), K_(table_id), K_(row_buf), K_(row_len));
};

// ---------------------------------------------------------------------------
// Execution context and subtask (created by Dispatcher, consumed by Worker)
// ---------------------------------------------------------------------------

/// Task execution context (shared by all subtasks: transaction handle, refresh scn, plugins).
class ObCSExecCtx
{
public:
  ObCSExecCtx() { reset(); }

  void reset()
  {
    task_count_ = 0;
    task_finish_ = 0;
    row_count_ = 0;
    refresh_scn_ = 0;
    schema_version_ = 0;
    plugins_ = nullptr;
    plugin_cnt_ = 0;
  }

  int64_t task_count_;
  int64_t task_finish_;
  int64_t row_count_;
  int64_t refresh_scn_;     // Advance this after batch commit.
  int64_t schema_version_;  // Tenant-level schema version.

  ObMySQLTransaction trans_;
  schema::ObSchemaGetterGuard schema_guard_;

  ObCSPlugin **plugins_ = nullptr;
  int64_t plugin_cnt_ = 0;
};

/// Subtask (sliced by rowkey hash; same rowkey is processed by one Worker serially).
class ObCSExecSubTask
{
public:
  ObCSExecSubTask() : slice_id_(-1), exec_ctx_(nullptr) {}

  int add_row(common::ObTabletID tablet_id, int64_t table_id,
              char *row_buf, int64_t row_len, int64_t commit_version);

  int64_t get_slice_id() const { return slice_id_; }
  void set_slice_id(int64_t id) { slice_id_ = id; }
  ObCSExecCtx *get_exec_ctx() const { return exec_ctx_; }
  void set_exec_ctx(ObCSExecCtx *ctx) { exec_ctx_ = ctx; }
  common::ObSEArray<ObCSRow, 4> &get_rows() { return rows_; }
  const common::ObSEArray<ObCSRow, 4> &get_rows() const { return rows_; }

private:
  int64_t slice_id_;
  ObCSExecCtx *exec_ctx_;
  common::ObSEArray<ObCSRow, 4> rows_;
};

// ---------------------------------------------------------------------------
// Row visibility (design doc 3.2)
// ---------------------------------------------------------------------------

/// Returns true if stmt_seq_no is not covered by any rollback to savepoint range.
inline bool is_row_visible(
    const transaction::ObTxSEQ &stmt_seq_no,
    const common::ObIArray<ObCSRollbackRange> &rollback_list)
{
  for (int64_t i = 0; i < rollback_list.count(); ++i) {
    const ObCSRollbackRange &r = rollback_list.at(i);
    if (r.first <= stmt_seq_no && stmt_seq_no < r.second) {
      return false;  // Inside a rollback range.
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// ObCSDispatcher: single-threaded consumer of committed tx, slices and pushes to Workers.
// ---------------------------------------------------------------------------

class ObCSWorker;  // Forward declaration; defined in ob_change_stream_worker.h

class ObCSDispatcher : public share::ObThreadPool
{
public:
  ObCSDispatcher();
  virtual ~ObCSDispatcher();

  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  /// Fetcher calls this when a transaction commits.
  int push(ObCSTxInfo *tx);

  int64_t get_change_stream_refresh_scn() const { return change_stream_refresh_scn_; }

protected:
  void run1() override;

private:
  int parse_redo_and_dispatch(ObCSTxInfo *tx);

private:
  bool is_inited_;
  common::ObExtendibleRingBuffer<ObCSTxInfo> tx_ring_;  // Stores ObCSTxInfo*
  int64_t change_stream_refresh_scn_;
  int64_t next_sn_;  // Serial number for ring buffer set/pop
};

}  // namespace share
}  // namespace oceanbase

#endif  // OB_CS_DISPATCHER_H_
