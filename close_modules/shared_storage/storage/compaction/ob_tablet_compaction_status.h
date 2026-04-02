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
#ifndef OB_STORAGE_COMPACTION_TABLET_COMPACTION_STATUS_H_
#define OB_STORAGE_COMPACTION_TABLET_COMPACTION_STATUS_H_
#include "share/compaction/ob_compaction_obj_interface.h"
#include "common/ob_tablet_id.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
namespace oceanbase
{
namespace compaction
{
class ObTabletCompactionStatus : public ObCompactionObjInterface
{
public:
  enum ObMajorCkpType : uint8_t {
    PARALLEL_TASK_IDX = 0,
    CG_IDX,
    CKM_TYPE_MAX
  };
  ObTabletCompactionStatus(
    const ObTabletID &tablet_id,
    const int64_t compaction_scn,
    const ObMajorCkpType ckp_type)
   : version_(TABLET_COMPACTION_STATUS_VERSION_V1),
     ckp_type_(ckp_type),
     reserved_(0),
     tablet_id_(tablet_id),
     compaction_scn_(compaction_scn),
     task_idx_(INVALID_TASK_IDX),
     root_macro_seq_(0)
   {}
  virtual bool is_valid() const override {
    return tablet_id_.is_valid() && compaction_scn_ > 0 && root_macro_seq_ >= 0;
  }
  TO_STRING_KV(K_(tablet_id), K_(compaction_scn), K_(task_idx), K_(ckp_type),
               K_(root_macro_seq));

protected:
  VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const override;
public:
  static const int64_t TABLET_COMPACTION_STATUS_VERSION_V1 = 1;
  static const int32_t SRCS_ONE_BYTE = 8;
  static const int32_t SRCS_FOUR_BITS = 4;
  static const int32_t SRCS_RESERVED_BITS = 56;
  union {
    uint64_t info_;
    struct {
      uint64_t version_   : SRCS_ONE_BYTE;
      uint64_t ckp_type_  : SRCS_FOUR_BITS;
      uint64_t reserved_  : SRCS_RESERVED_BITS;
    };
  };
  common::ObTabletID tablet_id_;
  int64_t compaction_scn_;
  // task_idx_ = parallel_idx [row_store]
  //             cg_idx [col_store]
  int64_t task_idx_;
  int64_t root_macro_seq_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TABLET_COMPACTION_STATUS_H_
