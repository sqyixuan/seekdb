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
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/tx_storage/ob_ls_service.h"
namespace oceanbase
{
using namespace storage;
using namespace common;
namespace compaction
{
const static char * ObExecModeStr[] = {
  "READ_ONLY",
  "WRITE_ONLY"
};

bool is_valid_obj_exec_mode(const ObjExecMode &exec_mode)
{
  return exec_mode >= OBJ_EXEC_MODE_READ_ONLY && exec_mode < OBJ_EXEC_MODE_MAX;
}

const char *exec_mode_to_str(const ObjExecMode &exec_mode)
{
  STATIC_ASSERT(static_cast<int64_t>(OBJ_EXEC_MODE_MAX) == ARRAYSIZEOF(ObExecModeStr), "obj exec mode str len is mismatch");
  const char *str = "";
  if (is_valid_obj_exec_mode(exec_mode)) {
    str = ObExecModeStr[exec_mode];
  } else {
    str = "invalid_obj_exec_mode";
  }
  return str;
}

const static char * ObCompactionStateStr[] = {
    "IDLE",
    "COMPACT",
    "CALC_CKM",
    "REPLICA_VERIFY",
    "INDEX_VERIFY",
    "RS_VERIFY",
    "REFRESH"
};

bool is_valid_compaction_state(const ObLSCompactionState &state) {
  return state >= COMPACTION_STATE_IDLE && state < COMPACTION_STATE_MAX;
}

const char *compaction_state_to_str(const ObLSCompactionState &state)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_STATE_MAX) == ARRAYSIZEOF(ObCompactionStateStr), "compaction state str len is mismatch");
  const char *str = "";
  if (is_valid_compaction_state(state)) {
    str = ObCompactionStateStr[state];
  } else {
    str = "invalid_state";
  }
  return str;
}

const static char * ObCompactionObjTypeStr[] = {
  "COMPACTION_SVRS",
  "LS_COMPACTION_STATUS",
  "LS_SVR_COMPACTION_STATUS",
  "SVR_ID_CACHE",
  "COMPACTION_REPORT",
  "LS_COMPACTION_TABLET_LIST"
};

bool is_valid_compaction_obj_type(const ObCompactionObjType &obj_type)
{
  return obj_type >= COMPACTION_SVRS && obj_type < COMPACTION_OBJ_TYPE_MAX;
}

const char *compaction_obj_type_to_str(const ObCompactionObjType &obj_type)
{
  STATIC_ASSERT(static_cast<int64_t>(COMPACTION_OBJ_TYPE_MAX) == ARRAYSIZEOF(ObCompactionObjTypeStr), "compaction obj type str len is mismatch");
  const char *str = "";
  if (is_valid_compaction_obj_type(obj_type)) {
    str = ObCompactionObjTypeStr[obj_type];
  } else {
    str = "invalid_obj_type";
  }
  return str;
}

int get_tablet_ids(
  const ObLSID &ls_id,
  ObIArray<ObTabletID> &tablet_ids)
{
  ObLSHandle ls_handle;
  return get_tablet_ids(ls_id, ls_handle, tablet_ids);
}

int get_tablet_ids(const ObLSID &ls_id, ObLSHandle &ls_handle, ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_all_tablet_ids(true/*except_ls_inner_tablet*/, tablet_ids))) {
    LOG_WARN("failed to get tablet id", K(ret), K(ls_id));
  }
  return ret;
}

int get_tablet_id_cnt(const share::ObLSID &ls_id, int64_t &tablet_cnt)
{
  int ret = OB_SUCCESS;
  tablet_cnt = 0;
  ObSEArray<ObTabletID, 1000> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(MTL_ID(), "ObjTabletIDs"));
  if (OB_FAIL(get_tablet_ids(ls_id, tablet_ids))) {
    LOG_WARN("failed to get tablet ids", KR(ret), K(ls_id));
  } else {
    tablet_cnt = tablet_ids.count();
  }
  return ret;
}
/**
 * -------------------------------------------------------------------ObVirtualTableInfo-------------------------------------------------------------------
 */

ObVirtualTableInfo::ObVirtualTableInfo()
  : ls_id_(),
    tablet_id_(),
    type_(COMPACTION_OBJ_TYPE_MAX),
    last_refresh_ts_(0),
    buf_(NULL)
{}

int ObVirtualTableInfo::init(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf is unexpected not null", KR(ret), KP_(buf));
  } else if (OB_ISNULL(buf_ = (char *)allocator.alloc(sizeof(char) * OB_MAX_VARCHAR_LENGTH))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", KR(ret), K(OB_MAX_VARCHAR_LENGTH));
  }
  return ret;
}

void ObVirtualTableInfo::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  type_ = COMPACTION_OBJ_TYPE_MAX;
  last_refresh_ts_ = 0;
  if (OB_NOT_NULL(buf_)) {
    MEMSET(buf_, '\0', OB_MAX_VARCHAR_LENGTH);
  }
}

void ObVirtualTableInfo::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(buf_)) {
    allocator.free(buf_);
    buf_ = NULL;
  }
}

int64_t ObVirtualTableInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    J_OBJ_START();
    J_KV(K_(ls_id), K_(tablet_id), K_(type), K_(last_refresh_ts));
    J_COMMA();
    if (OB_NOT_NULL(buf_)) {
      J_KV(KCSTRING_(buf));
    }
    J_OBJ_END();
  }
  return pos;
}

bool ObVirtualTableInfo::is_valid() const
{
  return is_valid_compaction_obj_type(type_);
}

} // namespace compaction
} // namespace oceanbase
