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
#include "ob_ls_compaction_list.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_ls_compaction_obj_mgr.h"

namespace oceanbase
{
using namespace share;
using namespace lib;
namespace compaction
{
ObLSCompactionListObj::ObLSCompactionListObj()
  : ObCompactionObjInterface(),
    version_(COMPACTION_LIST_VERSION_V1),
    reserved_(0),
    lock_(),
    ls_id_(),
    compaction_scn_(0),
    loop_cnt_(0),
    skip_merge_tablets_()
{
}

ObLSCompactionListObj::~ObLSCompactionListObj()
{
  destroy();
}

void ObLSCompactionListObj::destroy()
{
  is_inited_ = false;
  info_ = 0;
  ls_id_.reset();
  skip_merge_tablets_.destroy();
}

int ObLSCompactionListObj::init(const share::ObLSID &input_ls_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSCompactionListObj has been inited", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!input_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(input_ls_id));
  } else if (OB_FAIL(skip_merge_tablets_.create(DEFAULT_BUCKET_CNT, lib::ObMemAttr(MTL_ID(), "SkipMrgTblMap")))) {
    LOG_WARN("failed to create skip merge set", K(ret));
  } else if (FALSE_IT(ls_id_ = input_ls_id)) {
  } else if (OB_FAIL(try_reload_obj(true/*alloc_big_buf*/))) {
    LOG_WARN("failed to reload obj", K(ret), K(input_ls_id));
  } else if (is_reload_obj()) {
    // do nothing
  } else {
    compaction_scn_ = ObBasicMergeScheduler::INIT_COMPACTION_SCN;
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

bool ObLSCompactionListObj::is_valid() const
{
  return is_inited_
      && ls_id_.is_valid()
      && compaction_scn_ >= ObBasicMergeScheduler::INIT_COMPACTION_SCN
      && skip_merge_tablets_.created();
}

int ObLSCompactionListObj::refresh(
    const bool is_ls_leader,
    ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSCompactionListObj not inited", K(ret));
  } else if (is_ls_leader) { // leader: write obj to s2
    if (OB_FAIL(write_object(obj_buf))) {
      LOG_WARN("failed to write ls compaction list to s2", K(ret), K(ls_id_), K(compaction_scn_));
    } else {
      LOG_INFO("[SS_MERGE] succ to write ls compaction list", K(ret), KPC(this));
    }
  } else {
    const int64_t old_compaction_scn = compaction_scn_;
    obsys::ObWLockGuard guard(lock_);
    if (OB_FAIL(read_object(obj_buf))) {
      LOG_WARN("failed to read ls compaction list from s2", K(ret), K(ls_id_), K(compaction_scn_));
    } else {
      LOG_INFO("[SS_MERGE] succ to refresh ls compaction list", K(ret), KPC(this));
    }
  }
  ++loop_cnt_;
  return ret;
}

int ObLSCompactionListObj::tablet_need_skip(
    const int64_t merge_version,
    const ObTabletID &tablet_id,
    bool &need_skip) const
{
  int ret = OB_SUCCESS;
  need_skip = false;

  if (merge_version == compaction_scn_) {
    int hash_ret = skip_merge_tablets_.exist_refactored(tablet_id);
    if (OB_HASH_NOT_EXIST == hash_ret) {
      // should not skip
    } else if (OB_HASH_EXIST == hash_ret) {
      need_skip = true;
    } else {
      ret = hash_ret;
      LOG_WARN("failed to check merge list", K(ret), K(tablet_id), KPC(this));
    }
  }
  return ret;
}

int ObLSCompactionListObj::add_skip_tablet(
    const int64_t merge_version,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSCompactionListObj not inited", K(ret));
  } else if (OB_UNLIKELY(merge_version < compaction_scn_ || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(merge_version), K(tablet_id), KPC(this));
  } else {
    obsys::ObWLockGuard guard(lock_);
    if (merge_version > compaction_scn_) {
      skip_merge_tablets_.reuse();
      compaction_scn_ = merge_version;
    }
    (void) skip_merge_tablets_.set_refactored(tablet_id);
  }
  return ret;
}

void ObLSCompactionListObj::fill_info(ObVirtualTableInfo &info) const
{
  obsys::ObRLockGuard guard(lock_);
  info.type_ = LS_COMPACTION_TABLET_LIST;
  info.ls_id_ = ls_id_;
  ADD_COMPACTION_INFO_PARAM(info.buf_,
                            OB_MAX_VARCHAR_LENGTH,
                            K_(compaction_scn),
                            "skip_merge_count", skip_merge_tablets_.size(),
                            K_(loop_cnt));
}

void ObLSCompactionListObj::set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const
{
  opt.set_ss_compaction_scheduler_object_opt(blocksstable::ObStorageObjectType::LS_COMPACTION_LIST, ls_id_.id());
}

int ObLSCompactionListObj::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, new_pos, info_))) {
    STORAGE_LOG(WARN, "failed to serialize info", K(ret), K(info_));
  } else if (OB_FAIL(ls_id_.serialize(buf, buf_len, new_pos))) {
    LOG_WARN("failed to serialize ls id", K(ret), K(buf_len), K(new_pos), K(ls_id_));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, compaction_scn_))) {
    LOG_WARN("failed to serialize compaction scn", K(ret), K(buf_len), K(new_pos), K(compaction_scn_));
  } else {
    const int64_t skip_tablet_cnt = skip_merge_tablets_.size();
    if (FAILEDx(serialization::encode_i64(buf, buf_len, new_pos, skip_tablet_cnt))) {
      LOG_WARN("failed to serialize compaction scn", K(ret), K(buf_len), K(new_pos), K(skip_tablet_cnt));
    }
    SkipSet::const_iterator iter = skip_merge_tablets_.begin();
    for ( ; OB_SUCC(ret) && iter != skip_merge_tablets_.end(); ++iter) {
      const ObTabletID &tablet_id = iter->first;
      if (OB_FAIL(tablet_id.serialize(buf, buf_len, new_pos))) {
        LOG_WARN("failed to serialize tablet id", K(ret), K(buf_len), K(new_pos), K(tablet_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

int ObLSCompactionListObj::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int32_t tmp_info = 0;
  int64_t skip_tablet_cnt = 0;

  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi32(buf, data_len, new_pos, &tmp_info))) {
    LOG_WARN("failed to deserialize info", K(ret));
  } else if (FALSE_IT(info_ = static_cast<uint32_t>(tmp_info))) {
  } else if (OB_FAIL(ls_id_.deserialize(buf, data_len, new_pos))) {
    LOG_WARN("failed to deserialize ls id", K(ret), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &compaction_scn_))) {
    LOG_WARN("failed to deserialize compaction scn", K(ret), K(data_len));
  } else if (!skip_merge_tablets_.created()
          && skip_merge_tablets_.create(DEFAULT_BUCKET_CNT, lib::ObMemAttr(MTL_ID(), "SkipMrgTblMap"))) {
    LOG_WARN("failed to create skip merge set", K(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &skip_tablet_cnt))) {
    LOG_WARN("failed to deserialize compaction scn", K(ret), K(data_len));
  } else if (OB_UNLIKELY(skip_tablet_cnt < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array cnt is invalid", K(ret), K(skip_tablet_cnt));
  } else {
    skip_merge_tablets_.reuse();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < skip_tablet_cnt; ++idx) {
      ObTabletID tablet_id;
      if (OB_FAIL(tablet_id.deserialize(buf, data_len, new_pos))) {
        LOG_WARN("failed to deserialize tablet id", K(ret), K(data_len));
      } else if (OB_FAIL(skip_merge_tablets_.set_refactored(tablet_id, 1/*overwrite*/))) {
        LOG_WARN("failed to add skip merge tablet", K(ret), K(tablet_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    pos = new_pos;
    is_inited_ = true;
    FLOG_INFO("success to deserialize ObLSCompactionListObj", K(ret), KPC(this),
              "skip_merge_cnt", skip_merge_tablets_.size());
  }
  return ret;
}

int64_t ObLSCompactionListObj::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi32(info_);
  len += ls_id_.get_serialize_size();
  len += serialization::encoded_length_i64(compaction_scn_);
  len += serialization::encoded_length_i64(skip_merge_tablets_.size());

  SkipSet::const_iterator merge_iter = skip_merge_tablets_.begin();
  for ( ; merge_iter != skip_merge_tablets_.end(); ++merge_iter) {
    const ObTabletID &tablet_id = merge_iter->first;
    len += tablet_id.get_serialize_size();
  }
  return len;
}


} // comapction
} // oceanbase

