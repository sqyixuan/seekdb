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
#include "storage/compaction/ob_ls_merge_schedule_iterator.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;

namespace compaction
{

ObLSMergeIterator::ObLSMergeIterator()
  : merge_version_(0),
    ls_idx_(-1),
    ls_ids_()
{
  ls_ids_.set_attr(ObMemAttr(MTL_ID(), "SSMrgSchdIter"));
}

void ObLSMergeIterator::reset()
{
  merge_version_ = 0;
  ls_idx_ = -1;
  ls_ids_.reuse();
}

int ObLSMergeIterator::build_iter(const int64_t merge_version)
{
  int ret = OB_SUCCESS;
  reset();

  if (OB_FAIL(MTL(ObLSService *)->get_ls_ids(ls_ids_))) {
    LOG_WARN("failed to get all ls id", K(ret));
  } else {
    merge_version_ = merge_version;
    ls_idx_ = 0;
  }
  return ret;
}

int ObLSMergeIterator::get_next_ls(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  do {
    if (ls_idx_ >= ls_ids_.count()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_ids_[ls_idx_], ls_handle, ObLSGetMod::STORAGE_MOD))) {
      if (OB_LS_NOT_EXIST == ret) {
        LOG_TRACE("ls not exist", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
        ++ls_idx_;
      } else {
        LOG_WARN("failed to get ls", K(ret), K(ls_idx_), K(ls_ids_[ls_idx_]));
      }
    } else {
      ++ls_idx_;
    }
  } while (OB_LS_NOT_EXIST == ret);

  return ret;
}


} // compaction
} // oceanbase
