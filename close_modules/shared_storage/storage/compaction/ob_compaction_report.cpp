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
#include "storage/compaction/ob_compaction_report.h"
#include "storage/compaction/ob_tenant_ls_merge_scheduler.h"

namespace oceanbase
{
using namespace lib;
using namespace blocksstable;
namespace compaction
{
ObCompactionReportObj::ObCompactionReportObj()
  : version_(COMPACTION_REPORT_VERSION_V1),
    reserved_(0),
    errno_(OB_SUCCESS),
    report_ts_(0),
    finish_tablet_cnt_(0),
    exec_tablet_cnt_(0),
    unfinish_tablet_cnt_(0),
    lock_(),
    svr_id_(0),
    exec_mode_(ObjExecMode::OBJ_EXEC_MODE_MAX)
{}

ObCompactionReportObj::~ObCompactionReportObj()
{
  reset();
}

void ObCompactionReportObj::reset()
{
  ObMutexGuard guard(lock_);
  svr_id_ = 0;
  errno_ = OB_SUCCESS;
  report_ts_ = 0;
  finish_tablet_cnt_ = 0;
  exec_tablet_cnt_ = 0;
  unfinish_tablet_cnt_ = 0;
  exec_mode_ = ObjExecMode::OBJ_EXEC_MODE_MAX;
}

int ObCompactionReportObj::init(
  const uint64_t input_svr_id,
  const ObjExecMode exec_mode)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(0 == input_svr_id || !is_valid_obj_exec_mode(exec_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_svr_id));
  } else {
    ObMutexGuard guard(lock_);
    svr_id_ = input_svr_id;
    exec_mode_ = exec_mode;
  }

  if (FAILEDx(try_reload_obj())) {
    LOG_WARN("failed to reload obj", KR(ret), K(svr_id_), K(exec_mode_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

bool ObCompactionReportObj::is_valid() const
{
  return svr_id_ > 0 && report_ts_ > 0 && is_valid_obj_exec_mode(exec_mode_);
}

int ObCompactionReportObj::refresh(
  const bool is_leader,
  ObCompactionObjBuffer &obj_buf)
{
  UNUSED(is_leader);
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (OBJ_EXEC_MODE_READ_ONLY == exec_mode_) {
    if (OB_FAIL(read_object(obj_buf))) {
      if (OB_OBJECT_NOT_EXIST == ret) {
        // TODO file not exist on shared_storage, use correct errno later
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to read compaction report obj", KR(ret), K_(svr_id));
      }
    } else {
      LOG_INFO("[SharedStorage] success to read compaction report obj", KR(ret), K_(svr_id));
    }
  } else if (OBJ_EXEC_MODE_WRITE_ONLY == exec_mode_) {
    bool merge_finish = MERGE_SCHEDULER_PTR->get_inner_table_merged_scn() == MERGE_SCHEDULER_PTR->get_frozen_version();

    if (unfinish_tablet_cnt_ > 0 && merge_finish) {
      unfinish_tablet_cnt_ = 0;
      exec_tablet_cnt_ = 0;
    }

    if (OB_FAIL(write_object(obj_buf))) {
      LOG_WARN("failed to write compaction report obj", KR(ret), K_(svr_id));
    } else {
      LOG_INFO("[SharedStorage] success to write compaction report obj", KR(ret), K_(svr_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj exec mode", KR(ret), K_(exec_mode));
  }
  return ret;
}

int ObCompactionReportObj::update(const ObCompactionReportObj &refresh_obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!refresh_obj.is_valid() || refresh_obj.svr_id_ != svr_id_ || refresh_obj.report_ts_ < report_ts_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(svr_id), K(refresh_obj));
  } else if (!refresh_obj.is_empty()) {
    ObMutexGuard guard(lock_);
    info_ = refresh_obj.info_;
    errno_ = refresh_obj.errno_;
    report_ts_ = refresh_obj.report_ts_;
    finish_tablet_cnt_ = refresh_obj.finish_tablet_cnt_;
    exec_tablet_cnt_ = refresh_obj.exec_tablet_cnt_;
    unfinish_tablet_cnt_ = refresh_obj.unfinish_tablet_cnt_;
  }
  return ret;
}

int ObCompactionReportObj::update_exec_tablet(
    const int64_t exec_tablet_cnt,
    const bool is_finish_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 >= exec_tablet_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(exec_tablet_cnt));
  } else {
    ObMutexGuard guard(lock_);
    report_ts_ = ObTimeUtility::fast_current_time();
    exec_tablet_cnt_ = is_finish_task
                     ? MAX(0, exec_tablet_cnt_ - exec_tablet_cnt)
                     : exec_tablet_cnt_ + exec_tablet_cnt;
    unfinish_tablet_cnt_ = is_finish_task
                         ? MAX(0, unfinish_tablet_cnt_ - exec_tablet_cnt)
                         : unfinish_tablet_cnt_ + exec_tablet_cnt;
  }
  return ret;
}

void ObCompactionReportObj::fill_info(ObVirtualTableInfo &info)
{
  info.reset();
  info.ls_id_ = share::ObLSID(0);
  info.type_ = COMPACTION_REPORT;
  ObMutexGuard guard(lock_);
  info.last_refresh_ts_ = last_refresh_ts_;
  ADD_COMPACTION_INFO_PARAM(info.buf_, OB_MAX_VARCHAR_LENGTH, K_(errno), K_(report_ts), K_(finish_tablet_cnt),
    K_(exec_tablet_cnt), K_(unfinish_tablet_cnt));
}

OB_SERIALIZE_MEMBER_SIMPLE(ObCompactionReportObj,
    info_,
    errno_,
    report_ts_,
    finish_tablet_cnt_,
    exec_tablet_cnt_,
    unfinish_tablet_cnt_);

void ObCompactionReportObj::set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const
{
  opt.set_ss_compactor_svr_object_opt(ObStorageObjectType::COMPACTION_REPORT, svr_id_);
}

} // namespace compaction
} // namespace oceanbase
