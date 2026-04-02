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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_CONTEXT_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_CONTEXT_H_

#include "lib/container/ob_se_array.h"
#include "logservice/palf/lsn.h"
#include "ob_fetch_log_task.h"
#include "ob_log_restore_define.h"
#include <cstdint>
namespace oceanbase
{
namespace logservice
{
// The fetch log context of one ls,
// if the ls is scheduled with fetch log task,
// it is marked issued.
struct ObRemoteFetchContext
{
  int64_t issue_task_num_;
  int64_t issue_version_;
  int64_t last_fetch_ts_;     // last log fetch time
  palf::LSN max_submit_lsn_;        // Maximum LSN for submitting remote log fetch task
  palf::LSN max_fetch_lsn_;         // fetch to the last log end_lsn
  share::SCN max_fetch_scn_;  // pull to the last log log_ts
  ObLogRestoreErrorContext error_context_;          // Record the error information encountered by this log stream, valid only for leader
  common::ObSEArray<ObFetchLogTask *, 8> submit_array_;

  ObRemoteFetchContext() { reset(); }
  ~ObRemoteFetchContext() { reset(); }
  ObRemoteFetchContext &operator=(const ObRemoteFetchContext &other);
  void reset();
  int reset_sorted_tasks();
  void set_issue_version();
  TO_STRING_KV(K_(issue_task_num), K_(issue_version), K_(last_fetch_ts),
      K_(max_submit_lsn), K_(max_fetch_lsn), K_(max_fetch_scn),
      K_(error_context), "task_count", submit_array_.count());
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_CONTEXT_H_ */
