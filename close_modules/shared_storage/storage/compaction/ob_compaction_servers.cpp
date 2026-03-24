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
#include "ob_compaction_servers.h"
#include "src/share/ob_ls_id.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace obsys;
using namespace lib;
using namespace blocksstable;
namespace compaction
{
OB_SERIALIZE_MEMBER_SIMPLE(ObExecSvr, exec_svr_id_, compaction_scn_, report_ts_);
/**
 * -------------------------------------------------------------------ObCompactionServersObj-------------------------------------------------------------------
 */
ObCompactionServersObj::ObCompactionServersObj()
  : version_(COMPACTION_TASKS_VERSION_V1),
    reserved_(0),
    exec_svr_array_(),
    lock_(),
    ls_id_()
{}

ObCompactionServersObj::~ObCompactionServersObj()
{
  reset();
}

int ObCompactionServersObj::init(const share::ObLSID &input_ls_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCompactionServersObj has inited", KR(ret));
  } else if (OB_UNLIKELY(!input_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_ls_id));
  } else {
    ls_id_ = input_ls_id;
    exec_svr_array_.set_attr(ObMemAttr(MTL_ID(), "CompSvrs"));

    if (OB_FAIL(try_reload_obj())) {
      LOG_WARN("failed to try reload obj from s2", K(ret), K(input_ls_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObCompactionServersObj::reset()
{
  ObMutexGuard guard(lock_);
  ls_id_.reset();
  exec_svr_array_.reset();
}

int ObCompactionServersObj::refresh(
  const bool is_leader,
  ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  if (is_leader && !is_empty()) {
    ObMutexGuard guard(lock_);

    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObCompactionServersObj has not been inited", KR(ret));
    } else if (OB_FAIL(write_object(obj_buf))) {
      LOG_WARN("failed to write compaction server obj", KR(ret), K_(ls_id));
    } else {
      LOG_INFO("[SharedStorage] success to write compaction server obj", KR(ret), K_(ls_id), KPC(this));
    }
  } else {
    ObCompactionServersObj refresh_obj;
    if (OB_FAIL(refresh_obj.init(ls_id_))) { // will read when init
      LOG_WARN("failed to init refresh obj", KR(ret), K_(ls_id));
    } else if (OB_FAIL(update(refresh_obj))) {
      LOG_WARN("failed to update compaction server obj", KR(ret), K_(ls_id), K(refresh_obj));
    } else {
      LOG_INFO("[SharedStorage] success to read & update compaction server obj", KR(ret), K_(ls_id), K(refresh_obj));
    }
  }
  return ret;
}

int ObCompactionServersObj::update(const uint64_t exec_svr_id, const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  const int64_t exec_svr_cnt = exec_svr_array_.count();
  if (exec_svr_cnt > 0 &&
      compaction_scn == exec_svr_array_.at(exec_svr_cnt - 1).compaction_scn_ &&
      exec_svr_id == exec_svr_array_.at(exec_svr_cnt - 1).exec_svr_id_) {
    // exist same svr_id, no need update
  } else if (OB_FAIL(exec_svr_array_.push_back(ObExecSvr(exec_svr_id, compaction_scn)))) {
    LOG_WARN("failed to push back server id", KR(ret), K(exec_svr_id), K(compaction_scn));
  } else if (OB_FAIL(inner_update_report_obj())) {
    LOG_WARN("failed to update report obj", K(ret), K(exec_svr_id), K(compaction_scn));
  } else {
    LOG_INFO("success to add compaction server", KR(ret), K_(ls_id), K(exec_svr_id), K(compaction_scn), K_(exec_svr_array));
    ADD_ROLE_COMPACTION_EVENT(
        ObServerCompactionEvent::LS_LEADER,
        compaction_scn,
        ObServerCompactionEvent::CHOOSE_NEW_EXEC_SVR,
        ObTimeUtility::fast_current_time(),
        K_(ls_id),
        K(exec_svr_id));
  }
  return ret;
}

int ObCompactionServersObj::update(const ObCompactionServersObj &other)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  bool need_update_report_obj = false;

  if (OB_UNLIKELY(ls_id_ != other.ls_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls id for update", KR(ret), KPC(this), K(other));
  } else if (FALSE_IT(last_refresh_ts_ = other.last_refresh_ts_)) {
  } else if (other.exec_svr_array_.empty()) {
    // exist no new exec svr, do nothing
  } else if (exec_svr_array_.empty()) {
    need_update_report_obj = true;
  } else {
    const ObExecSvr &last_svr = exec_svr_array_.at(exec_svr_array_.count() - 1);
    const ObExecSvr &other_last_svr = other.exec_svr_array_.at(other.exec_svr_array_.count() - 1);

    if (last_svr.exec_svr_id_ != other_last_svr.exec_svr_id_) {
      need_update_report_obj = true;
    }
  }

  if (FAILEDx(exec_svr_array_.assign(other.exec_svr_array_))) {
    LOG_WARN("failed to assign exec svr array", KR(ret));
  } else if (need_update_report_obj && OB_FAIL(inner_update_report_obj())) {
    LOG_WARN("failed to update report obj", K(ret));
  }
  return ret;
}

// should be called under lock_
int ObCompactionServersObj::inner_update_report_obj()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(exec_svr_array_.empty())) {
    // do nothing
  } else if (GCTX.get_server_id() == exec_svr_array_.at(exec_svr_array_.count() - 1).exec_svr_id_) {
    ObBasicObjHandle<ObCompactionReportObj> report_obj_hdl;
    ObLSHandle ls_hdl;
    int64_t ls_tablet_cnt = 0;
    if (OB_FAIL(MTL_SVR_OBJ_MGR.get_obj_handle(GCTX.get_server_id(), report_obj_hdl))) {
      LOG_WARN("failed to get report obj handle", K(ret), "cur_svr_id", GCTX.get_server_id());
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_hdl, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls handle", K(ret), K_(ls_id));
    } else if (FALSE_IT(ls_tablet_cnt = ls_hdl.get_ls()->get_tablet_svr()->get_tablet_count())) {
    } else if (OB_FAIL(report_obj_hdl.get_obj()->update_exec_tablet(ls_tablet_cnt))) {
      LOG_WARN("failed to inc exec tablet", K(ret));
    }
  }
  return ret;
}

void ObCompactionServersObj::fill_info(ObLSBroadcastInfo &info)
{
  info.exec_svr_id_ = 0;
  ObMutexGuard guard(lock_);
  if (exec_svr_array_.count() > 0) {
    for (int64_t idx = exec_svr_array_.count() - 1; idx >= 0; --idx) {
      const ObExecSvr &exec_svr = exec_svr_array_.at(idx);
       if (info.compaction_scn_ == exec_svr.compaction_scn_) {
          info.exec_svr_id_ = exec_svr.exec_svr_id_;
          break;
       } else if (info.compaction_scn_ > exec_svr.compaction_scn_) {
        LOG_INFO("not found exec svr for specific compaction scn", K(info), K_(exec_svr_array), K(idx));
        break;
      }
    }
  }
}

void ObCompactionServersObj::fill_info(ObVirtualTableInfo &info)
{
  int ret = OB_SUCCESS;
  info.reset();
  info.ls_id_ = ls_id_;
  info.type_ = COMPACTION_SVRS;
  ObMutexGuard guard(lock_);
  info.last_refresh_ts_ = last_refresh_ts_;
  const int64_t array_cnt = get_serialize_array_cnt();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < array_cnt; ++idx) {
    ADD_COMPACTION_INFO_PARAM(info.buf_, OB_MAX_VARCHAR_LENGTH, "svr", exec_svr_array_.at(idx));
  }
}

int ObCompactionServersObj::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else {
    const int64_t start_idx = get_serialize_array_start_idx();
    const int64_t array_cnt = get_serialize_array_cnt();
    if (OB_FAIL(serialization::encode(buf, buf_len, new_pos, info_))) {
      LOG_WARN("failed to serialize info", K(ret), K(buf_len), K_(info));
    } else if (OB_FAIL(serialization::encode(buf, buf_len, new_pos, array_cnt))) {
      LOG_WARN("failed to serialize exec svr count", K(ret), K(buf_len), K(array_cnt));
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < array_cnt; ++idx) {
      const ObExecSvr &svr = exec_svr_array_.at(start_idx + idx);
      if (OB_FAIL(svr.serialize(buf, buf_len, new_pos))) {
        LOG_WARN("failed to serialize exec svr", K(ret), K(buf_len), K(svr));
      }
    } // end of for
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

int ObCompactionServersObj::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t array_cnt = 0;
  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, data_len, new_pos, info_))) {
    LOG_WARN("failed to deserialize array_cnt", K(ret), K(data_len));
  } else if (OB_FAIL(serialization::decode(buf, data_len, new_pos, array_cnt))) {
    LOG_WARN("failed to deserialize array_cnt", K(ret), K(data_len));
  } else if (OB_UNLIKELY(array_cnt < 0 || array_cnt > DEFAULT_ARRAY_CNT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array cnt is invalid", KR(ret), K(array_cnt));
  } else {
    ObExecSvr svr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < array_cnt; ++idx) {
      svr.reset();
      if (OB_FAIL(svr.deserialize(buf, data_len, new_pos))) {
        LOG_WARN("failed to deserialize exec svr", KR(ret), K(data_len), K(svr));
      } else if (OB_FAIL(exec_svr_array_.push_back(svr))) {
        LOG_WARN("failed to push svr", KR(ret), K(svr));
      }
    } // end of for
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

int64_t ObCompactionServersObj::get_serialize_size() const
{
  int64_t len = 0;
  const int64_t start_idx = get_serialize_array_start_idx();
  const int64_t array_cnt = get_serialize_array_cnt();
  len += serialization::encoded_length(info_);
  len += serialization::encoded_length(array_cnt);
  for (int64_t idx = 0; idx < array_cnt; ++idx) {
    len += exec_svr_array_.at(start_idx + idx).get_serialize_size();
  }
  return len;
}

void ObCompactionServersObj::set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const
{
  opt.set_ss_compaction_scheduler_object_opt(ObStorageObjectType::COMPACTION_SERVER, ls_id_.id());
}

} // namespace compaction
} // namespace oceanbase
