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
#define USING_LOG_PREFIX SHARE
#include "observer/virtual_table/ob_all_virtual_shared_storage_compaction_info.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"


namespace oceanbase
{
using namespace oceanbase;
using namespace common;
using namespace storage;
using namespace compaction;
namespace observer
{
ObAllVirtualSharedStorageCompactionInfo::ObAllVirtualSharedStorageCompactionInfo()
  : ObVirtualTableScannerIterator(),
    ls_ids_(),
    ls_idx_(0),
    loop_state_(STATE_MAX),
    allocator_(NULL),
    is_inited_(false)
{
}

ObAllVirtualSharedStorageCompactionInfo::~ObAllVirtualSharedStorageCompactionInfo()
{
  reset();
}

void ObAllVirtualSharedStorageCompactionInfo::reset()
{
  if (is_inited_) {
    omt::ObMultiTenantOperator::reset(); // will release last tenant
    ObVirtualTableScannerIterator::reset();
    info_.destroy(*allocator_);
    allocator_ = NULL;
    is_inited_ = false;
  }
}

int ObAllVirtualSharedStorageCompactionInfo::init(
  ObIAllocator *allocator, ObAddr addr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualSharedStorageCompactionInfo has been inited", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), KP(allocator));
  } else if (OB_FAIL(info_.init(*allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc tablet iter buf", K(ret));
  } else if (!addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "failed to push ip addr to string", K(ret));
  } else {
    addr_ = addr;
    allocator_ = allocator;
    loop_state_ = STATE_MAX;
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualSharedStorageCompactionInfo::prepare_ls_ids()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(STATE_MAX != loop_state_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid loop state", KR(ret), K_(loop_state));
  } else if (ls_ids_.empty()) {
    if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(ls_ids_))) {
      SERVER_LOG(WARN, "failed to get ls ids", KR(ret));
    } else {
      ls_idx_ = -1;
      tablet_idx_ = 0;
      tablet_ids_.reuse();
      loop_state_ = COMPACTION_SVRS;
    }
  }
  if (OB_SUCC(ret)) {
    do {
      if (++ls_idx_ >= ls_ids_.count()) {
        ret = OB_ITER_END;
      } else {
        const ObLSID &ls_id = ls_ids_[ls_idx_];
        if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_id, obj_handle_))) {
          if (OB_HASH_NOT_EXIST == ret || OB_LS_NOT_EXIST == ret) {
            SERVER_LOG(DEBUG, "ls info not exist", KR(ret), K(ls_id));
            ret = OB_SUCCESS;
          } else {
            SERVER_LOG(WARN, "failed to get obj", KR(ret), K(ls_id));
          }
        } else {
          break;
        }
      }
    } while (OB_SUCC(ret));
  }
  return ret;
}

void ObAllVirtualSharedStorageCompactionInfo::release_last_tenant()
{
  ls_ids_.reuse();
  tablet_ids_.reuse();
  loop_state_ = STATE_MAX;
}

int ObAllVirtualSharedStorageCompactionInfo::prepare_tablet_ids()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls_idx_ < 0 || ls_idx_ >= ls_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid ls idx", KR(ret), K_(ls_idx), K_(ls_ids));
  } else {
    tablet_ids_.reuse();
    if (OB_FAIL(get_tablet_ids(ls_ids_[ls_idx_], tablet_ids_))) {
      SERVER_LOG(WARN, "failed to get tablet id", K(ret), K_(ls_idx), "ls_id", ls_ids_[ls_idx_]);
    } else {
      tablet_idx_ = 0;
    }
  }
  return ret;
}

int ObAllVirtualSharedStorageCompactionInfo::fill_tablet_info(ObVirtualTableInfo &info)
{
  int ret = OB_SUCCESS;
  if (tablet_idx_ >= tablet_ids_.count()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!obj_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "obj handle is invalid", K(ret), K_(obj_handle));
  } else {
    ObTabletCompactionState tmp_state;
    do {
      const ObTabletID &tablet_id = tablet_ids_[tablet_idx_++];
      if (OB_FAIL(obj_handle_.get_obj()->get_tablet_state(tablet_id, tmp_state))) {
        if (OB_HASH_NOT_EXIST != ret) {
          SERVER_LOG(WARN, "failed to get tablet state", KR(ret), K(tablet_id));
        } else {
          SERVER_LOG(DEBUG, "tablet info not exist", KR(ret), K(tablet_id));
          ret = OB_SUCCESS;
        }
      } else {
        info.ls_id_ = ls_ids_[ls_idx_];
        info.tablet_id_ = tablet_id;
        info.type_ = ObCompactionObjType::LS_SVR_COMPACTION_STATUS;
        tmp_state.fill_info(info);
        SERVER_LOG(DEBUG, "success to fill tablet info", KR(ret), K(info));
      }
    } while (!info.is_valid() && tablet_idx_ < tablet_ids_.count() && OB_SUCC(ret));
  }
  if (OB_SUCC(ret) && tablet_idx_ >= tablet_ids_.count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualSharedStorageCompactionInfo::fill_ls_compaction_list_info(ObVirtualTableInfo &info)
{
  int ret = OB_SUCCESS;

  if (tablet_idx_ >= tablet_ids_.count()) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!obj_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "obj handle is invalid", K(ret), K_(obj_handle));
  } else {
    const int64_t compaction_scn = obj_handle_.get_obj()->ls_compaction_list_.get_compaction_scn();
    do {
      const ObTabletID &tablet_id = tablet_ids_[tablet_idx_++];
      bool need_skip = false;
      if (OB_SUCC(obj_handle_.get_obj()->ls_compaction_list_.tablet_need_skip(compaction_scn, tablet_id, need_skip))) {
        info.ls_id_ = ls_ids_[ls_idx_];
        info.tablet_id_ = tablet_id;
        info.type_ = ObCompactionObjType::LS_COMPACTION_TABLET_LIST;

        ADD_COMPACTION_INFO_PARAM(info.buf_,
                                  OB_MAX_VARCHAR_LENGTH,
                                  K(compaction_scn),
                                  "merge_status", need_skip ? "skip index verify" : "not skip");
        SERVER_LOG(DEBUG, "success to fill tablet info", KR(ret), K(info));
      }
    } while (!info.is_valid() && tablet_idx_ < tablet_ids_.count() && OB_SUCC(ret));
  }
  if (OB_SUCC(ret) && tablet_idx_ >= tablet_ids_.count()) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObAllVirtualSharedStorageCompactionInfo::fill_compaction_report(ObVirtualTableInfo &info)
{
  int ret = OB_SUCCESS;
  ObBasicObjHandle<ObCompactionReportObj> handle;
  const uint64_t cur_svr_id = GCTX.get_server_id();
  if (0 == cur_svr_id) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(MTL_SVR_OBJ_MGR.get_obj_handle(cur_svr_id, handle))) {
    if (OB_HASH_NOT_EXIST == ret) {
      SERVER_LOG(DEBUG, "svr info not exist", KR(ret), K(cur_svr_id));
      ret = OB_ITER_END;
    } else {
      SERVER_LOG(WARN, "failed to get obj", KR(ret));
    }
  } else {
    handle.get_obj()->fill_info(info);
  }
  return ret;
}

#define FILL_INFO(info, var) \
  obj_handle_.get_obj()->var##_.fill_info(info);

int ObAllVirtualSharedStorageCompactionInfo::get_info(ObVirtualTableInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  do {
    switch ((uint8_t)loop_state_) {
      case STATE_MAX:
        if (OB_SUCC(prepare_ls_ids())) {
          loop_state_ = COMPACTION_SVRS;
        }
        break;
      case COMPACTION_SVRS:
        FILL_INFO(info, compaction_svrs);
        loop_state_ = LS_COMPACTION_STATUS;
        break;
      case LS_COMPACTION_STATUS:
        FILL_INFO(info, ls_compaction_status);
        loop_state_ = LS_SVR_COMPACTION_STATUS;
        break;
      case LS_SVR_COMPACTION_STATUS:
        FILL_INFO(info, cur_svr_ls_compaction_status);
        if (OB_SUCC(prepare_tablet_ids())) {
          loop_state_ = LOOP_SVR_TABLET_STATUS;
        }
        break;
      case LOOP_SVR_TABLET_STATUS:
        if (OB_FAIL(fill_tablet_info(info))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            loop_state_ = COMPACTION_REPORT;
          }
        }
        break;
      case COMPACTION_REPORT:
        if (1 == obj_handle_.get_obj()->ls_compaction_status_.get_ls_id().id()) {
          // only show once
          fill_compaction_report(info);
        }
        loop_state_ = LS_COMPACTION_TALBET_LIST;
        break;
      case LS_COMPACTION_TALBET_LIST:
        FILL_INFO(info, ls_compaction_list);
        if (OB_SUCC(prepare_tablet_ids())) {
          loop_state_ = LOOP_TABLET_LIST_STATUS;
        }
        break;
      case LOOP_TABLET_LIST_STATUS:
        if (OB_FAIL(fill_ls_compaction_list_info(info))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            loop_state_ = STATE_MAX;
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid loop state", K(ret), K_(loop_state));
    }
    SERVER_LOG(DEBUG, "in while", KR(ret), K_(loop_state), K_(ls_idx), K_(ls_ids), K_(tablet_ids), K_(tablet_idx), K(info), KPC(obj_handle_.get_obj()));
  } while (!info.is_valid() && OB_SUCC(ret));
  SERVER_LOG(DEBUG, "get info", KR(ret), K_(loop_state), K(info));
  return ret;
}

int ObAllVirtualSharedStorageCompactionInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to execute", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualSharedStorageCompactionInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_info(info_))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "failed to get info", KR(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case LS_ID:
          cur_row_.cells_[i].set_int(info_.ls_id_.id());
          break;
        case TABLET_ID:
          cur_row_.cells_[i].set_int(info_.tablet_id_.id());
          break;
        case OBJ_TYPE:
          cur_row_.cells_[i].set_varchar(compaction_obj_type_to_str(info_.type_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case LAST_REFRESH_TIME:
          cur_row_.cells_[i].set_timestamp(info_.last_refresh_ts_);
          break;
        case INFO:
          cur_row_.cells_[i].set_varchar(info_.buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

} /* namespace observer */
} /* namespace oceanbase */
