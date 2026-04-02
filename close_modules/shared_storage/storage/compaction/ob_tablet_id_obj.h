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
#ifndef OB_STORAGE_COMPACTION_TABLET_ID_OBJ_H_
#define OB_STORAGE_COMPACTION_TABLET_ID_OBJ_H_
#include "share/compaction/ob_compaction_obj_interface.h"
#include "common/ob_tablet_id.h"
namespace oceanbase
{
namespace compaction
{

class ObTabletIDObj : public ObCompactionObjInterface
{
public:
  ObTabletIDObj(const common::ObTabletID &input_tablet_id);
  ObTabletIDObj(
    const common::ObTabletID &input_tablet_id,
    const int64_t compaction_scn,
    const int64_t last_major_root_macro_seq,
    const int64_t parallel_cnt,
    const int64_t cg_dir_cnt);
  ~ObTabletIDObj() { reset(); }
  void reset();
  virtual bool is_valid() const override;
  #define GET_FUNC(name) int64_t get_##name() const { return name##_; }
  GET_FUNC(compaction_scn);
  GET_FUNC(last_major_root_macro_seq);
  GET_FUNC(parallel_cnt);
  GET_FUNC(cg_dir_cnt);
  #undef GET_FUNC
  INHERIT_TO_STRING_KV("ObTabletIDObj", ObCompactionObjInterface, K_(tablet_id), K_(compaction_scn),
    K_(last_major_root_macro_seq), K_(parallel_cnt), K_(cg_dir_cnt));
protected:
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const override;
private:
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_RESERVED_BITS = 56;
  static const int64_t TABLET_ID_OBJ_VERSION_V1 = 1;
  union {
    uint64_t info_;
    struct {
      uint64_t version_   : SRCS_ONE_BYTE;
      uint64_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  common::ObTabletID tablet_id_;
  int64_t compaction_scn_;
  int64_t last_major_root_macro_seq_;
  int64_t parallel_cnt_;
  int64_t cg_dir_cnt_; // for row_store, cg_dir_cnt = 1; for col_store, cg_dir_cnt = column_group_count
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TABLET_ID_OBJ_H_
