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
#ifndef OB_STORAGE_COMPACTION_MAJOR_PRE_WARMER_H_
#define OB_STORAGE_COMPACTION_MAJOR_PRE_WARMER_H_
#include "share/storage/ob_i_pre_warmer.h"
#include "storage/shared_storage/prewarm/ob_mc_prewarm_struct.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "share/cache/ob_kvcache_pre_warmer.h"
namespace oceanbase
{
namespace compaction
{

enum class ObSSMajorPrewarmLevel : uint8_t
{
  PREWARM_META_AND_DATA_LEVEL = 0,
  PREWARM_ONLY_META_LEVEL = 1,
  PREWARM_NONE_LEVEL = 2,
  PREWARM_MAX_LEVEL = 3
};

template <typename T>
class ObMajorPreWarmer : public share::ObIPreWarmer
{
public:
  ObMajorPreWarmer(storage::ObHotTabletInfoBaseWriter &tablet_info_writer)
    : ObIPreWarmer(), tablet_info_writer_(tablet_info_writer),
      hot_tablet_info_(), time_guard_(20_min), micro_cnt_(0)
  {}
  virtual ~ObMajorPreWarmer() {}
  virtual int init(const storage::ObITableReadInfo *table_read_info) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "pre warmer init twice", KR(ret));
    // } else if (OB_FAIL(mem_pre_warmer_.init(table_read_info))) {
    //   STORAGE_LOG(WARN, "fail to init mem pre warmer", KR(ret));
    } else {
      is_inited_ = true;
    }
    return ret;
  }
  virtual void reuse() override { /*mem_pre_warmer_.reuse();*/ }
  virtual int reserve(const blocksstable::ObMicroBlockDesc &micro_block_desc,
                      bool &reserve_succ_flag,
                      const int64_t level = 0) override
  {
    int ret = OB_SUCCESS;
    reserve_succ_flag = false;
    if (IS_NOT_INIT) {
      // do nothing, and do not return errno
    // } else if (OB_FAIL(mem_pre_warmer_.reserve(micro_block_desc, reserve_succ_flag, level))) {
    //   STORAGE_LOG(WARN, "fail to reserve", KR(ret), K(micro_block_desc), K(level));
    }
    return ret;
  }

  virtual int add(const blocksstable::ObMicroBlockDesc &micro_block_desc, const bool reserve_succ_flag) override
  {
    int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    ObCompactionTimeGuard tmp_time_guard;
    if (IS_NOT_INIT) {
      // do nothing, and do not return errno
    } else {
      // if (OB_TMP_FAIL(mem_pre_warmer_.add(micro_block_desc, reserve_succ_flag))) {
      //   STORAGE_LOG(WARN, "failed to add mem pre warmer", KR(tmp_ret), K(micro_block_desc));
      // }

      if (OB_TMP_FAIL(hot_tablet_info_.push_back(micro_block_desc, tablet_info_writer_))) {
        STORAGE_LOG(WARN, "failed to add pre warmer for shared storage", KR(tmp_ret), K(micro_block_desc));
      }

      if (OB_SUCC(ret)) {
        ++micro_cnt_;
        tmp_time_guard.click(ObStorageCompactionTimeGuard::PRE_WARM);
        time_guard_.add_time_guard(tmp_time_guard);
      }
    }
    return ret;
  }

  virtual int close() override
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      // do nothing, and do not return errno
    } else if (hot_tablet_info_.count() > 0 && OB_FAIL(tablet_info_writer_.append(hot_tablet_info_.hot_macro_infos_))) {
      STORAGE_LOG(WARN, "failed to write info", KR(ret), K(hot_tablet_info_));
    } else {
      STORAGE_LOG(INFO, "success to close ObMajorPreWarmer", KR(ret), K_(time_guard), K_(micro_cnt));
    }
    return ret;
  }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(hot_tablet_info), K_(time_guard), K_(micro_cnt));
private:
  // T mem_pre_warmer_;
  storage::ObHotTabletInfoBaseWriter &tablet_info_writer_;
  storage::ObHotTabletInfo hot_tablet_info_;
  ObStorageCompactionTimeGuard time_guard_;
  int64_t micro_cnt_;
};

struct ObMajorPreWarmerParam : public share::ObPreWarmerParam
{
  ObMajorPreWarmerParam(storage::ObHotTabletInfoWriter &input_writer)
    : ObPreWarmerParam(share::MEM_AND_FILE_PRE_WARM),
      is_inited_(false),
      pre_warm_writer_(input_writer),
      pre_warm_level_(ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL)
  {}
  virtual ~ObMajorPreWarmerParam() {}
  virtual bool is_valid() const override;
  virtual int init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id, const bool use_fixed_percentage = false) override;
  INHERIT_TO_STRING_KV("ObMajorPreWarmerParam", ObPreWarmerParam, K_(type));
  bool is_inited_;
  storage::ObHotTabletInfoWriter &pre_warm_writer_;
  ObSSMajorPrewarmLevel pre_warm_level_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MAJOR_PRE_WARMER_H_
