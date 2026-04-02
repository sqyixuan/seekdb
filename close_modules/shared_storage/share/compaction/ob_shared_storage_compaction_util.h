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
#ifndef OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
#define OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
#include "stdint.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
namespace oceanbase
{
namespace storage
{
class ObLSHandle;
}
namespace compaction
{
enum ObjExecMode : uint8_t
{
  OBJ_EXEC_MODE_READ_ONLY = 0,
  OBJ_EXEC_MODE_WRITE_ONLY = 1,
  OBJ_EXEC_MODE_MAX
};

bool is_valid_obj_exec_mode(const ObjExecMode &exec_mode);
const char *exec_mode_to_str(const ObjExecMode &exe_mode);

enum ObLSCompactionState : uint8_t
{
  COMPACTION_STATE_IDLE = 0,
  COMPACTION_STATE_COMPACT,
  COMPACTION_STATE_CALC_CKM,
  COMPACTION_STATE_REPLICA_VERIFY,
  COMPACTION_STATE_INDEX_VERIFY,
  COMPACTION_STATE_RS_VERIFY, // wait for RS to verify special table
  COMPACTION_STATE_REFRESH,
  COMPACTION_STATE_MAX
};
bool is_valid_compaction_state(const ObLSCompactionState &state);
const char *compaction_state_to_str(const ObLSCompactionState &merge_type);

int get_tablet_ids(const share::ObLSID &ls_id, common::ObIArray<ObTabletID> &tablet_ids);
int get_tablet_ids(const share::ObLSID &ls_id, storage::ObLSHandle &ls_handle, common::ObIArray<ObTabletID> &tablet_ids);
int get_tablet_id_cnt(const share::ObLSID &ls_id, int64_t &tablet_cnt);

/**
 * -------------------------------------------------------------------virtual table section-------------------------------------------------------------------
 */
enum ObCompactionObjType : uint8_t
{
  COMPACTION_SVRS = 0,
  LS_COMPACTION_STATUS,
  LS_SVR_COMPACTION_STATUS,
  SVR_ID_CACHE,
  COMPACTION_REPORT,
  LS_COMPACTION_TABLET_LIST,
  COMPACTION_OBJ_TYPE_MAX,
};
bool is_valid_compaction_obj_type(const ObCompactionObjType &obj_type);
const char *compaction_obj_type_to_str(const ObCompactionObjType &obj_type);

struct ObVirtualTableInfo
{
  ObVirtualTableInfo();
  int init(ObIAllocator &allocator);
  void reset();
  void destroy(ObIAllocator &allocator);
  bool is_valid() const;
  DECLARE_TO_STRING;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObCompactionObjType type_;
  int64_t last_refresh_ts_;
  char *buf_;
};

static const char *SHARED_STORAGE_REPLICA_CKM_SVR_IP = "0.0.0.0";
static const int SHARED_STORAGE_REPLICA_CKM_SVR_PORT = 0;
static const int64_t MACRO_STEP_SIZE = 0x1 << 25;
static const int64_t INVALID_TASK_IDX = -1;

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
