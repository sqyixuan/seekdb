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

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_tmp_file_io_callback.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;

ObSSTmpFileIOCallback::ObSSTmpFileIOCallback()
    : ObIOCallback(ObIOCallbackType::SS_TMP_FILE_CALLBACK), seg_id_(), meta_handle_(),
      seg_meta_op_type_(ObSSTmpFileSegMetaOpType::INSERT), seg_del_type_(ObSSTmpFileSegDeleteType::NONE),
      del_seg_valid_len_(0), allocator_(nullptr)
{
}

int ObSSTmpFileIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  UNUSED(data_buffer);
  UNUSED(size);
  // Note:: although do not support concurrent read and write of one same segment, try lock here.
  TmpFileMetaHandle meta_handle;
  bool is_meta_exist = false;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager->get_segment_file_mgr().try_get_seg_meta(seg_id_, meta_handle, is_meta_exist))) {
    LOG_WARN("fail to try get seg meta", KR(ret), K_(seg_id));
  } else if (is_meta_exist) {
    if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta handle is in valid", KR(ret), K(meta_handle));
    } else {
      SpinWLockGuard guard(meta_handle.get_tmpfile_meta()->lock_);
      inner_process_();
    }
  } else { // !is_meta_exist
    inner_process_();
  }
  return ret;
}

int ObSSTmpFileIOCallback::inner_process_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(process_seg_meta())) {
    LOG_WARN("fail to process seg meta", KR(ret), KPC(this));
  } else if (OB_FAIL(process_seg_deletion())) {
    LOG_WARN("fail to process seg deletion", KR(ret), KPC(this));
  }
  return ret;
}

int ObSSTmpFileIOCallback::set_ss_tmpfile_io_callback(const TmpFileSegId &seg_id,
                                                      const TmpFileMetaHandle &meta_handle,
                                                      const ObSSTmpFileSegMetaOpType seg_meta_op_type,
                                                      const ObSSTmpFileSegDeleteType seg_del_type,
                                                      const int64_t del_seg_valid_len,
                                                      common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_handle.is_valid() ||
                  !seg_id.is_valid() ||
                  (((ObSSTmpFileSegDeleteType::REMOTE == seg_del_type) ||
                    (ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE == seg_del_type)) &&
                   (del_seg_valid_len <= 0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta handle", KR(ret), K(meta_handle), K(seg_id), K(seg_del_type), K(del_seg_valid_len));
  } else if (OB_FAIL(meta_handle_.assign(meta_handle))) {
    LOG_WARN("fail to assign meta handle", KR(ret), K(meta_handle));
  } else {
    type_ = ObIOCallbackType::SS_TMP_FILE_CALLBACK;
    seg_id_ = seg_id;
    seg_meta_op_type_ = seg_meta_op_type;
    seg_del_type_ = seg_del_type;
    del_seg_valid_len_ = del_seg_valid_len;
    allocator_ = allocator;
  }
  return ret;
}

int ObSSTmpFileIOCallback::process_seg_meta()
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else {
    switch (seg_meta_op_type_) {
      case ObSSTmpFileSegMetaOpType::INSERT: {
        ObSegmentFileManager &segment_file_manager = file_manager->get_segment_file_mgr();
        if (OB_FAIL(segment_file_manager.insert_meta(seg_id_, meta_handle_))) {
          LOG_WARN("fail to insert meta", KR(ret), K_(seg_id), K_(meta_handle));
        }
        break;
      }
      case ObSSTmpFileSegMetaOpType::DELETE: {
        ObSegmentFileManager &segment_file_manager = file_manager->get_segment_file_mgr();
        // Note: although there is no concurrent read and write, set is_in_local_ = false here.
        bool is_meta_exist = false;
        TmpFileMetaHandle meta_handle;
        if (OB_FAIL(segment_file_manager.try_get_seg_meta(seg_id_, meta_handle, is_meta_exist))) {
          LOG_WARN("fail to try get seg meta", KR(ret), K_(seg_id));
        } else if (!is_meta_exist) {
          // do nothing. may be gc concurrently
        } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("meta handle should not be invalid", KR(ret), K_(seg_id), K(meta_handle));
        } else {
          // Note: already wlock outside, cannot wlock again here
          meta_handle.get_tmpfile_meta()->is_in_local_ = false;
          if (OB_FAIL(segment_file_manager.delete_meta(seg_id_))) {
            LOG_WARN("fail to delete meta", KR(ret), K_(seg_id));
          }
        }
        break;
      }
      case ObSSTmpFileSegMetaOpType::UPDATE: {
        ObSegmentFileManager &segment_file_manager = file_manager->get_segment_file_mgr();
        if (OB_FAIL(segment_file_manager.update_meta(seg_id_, meta_handle_))) {
          LOG_WARN("fail to update meta", KR(ret), K_(seg_id), K_(meta_handle));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected segment meta operation type", KR(ret), K_(seg_meta_op_type));
        break;
      }
    }
  }
  return ret;
}

int ObSSTmpFileIOCallback::process_seg_deletion()
{
  int ret = OB_SUCCESS;
  switch (seg_del_type_) {
    case ObSSTmpFileSegDeleteType::NONE: {
      // do nothing
      break;
    }
    case ObSSTmpFileSegDeleteType::LOCAL: {
      // use OB_FAIL. if delete seg file failed, then make this whole write io failed.
      // so as to make local cache tmp file is always newest, and we can read local file in any time.
      if (OB_FAIL(delete_local_seg_file(seg_id_))) {
        LOG_WARN("fail to delete local seg file", KR(ret), K_(seg_id));
      }
      break;
    }
    case ObSSTmpFileSegDeleteType::REMOTE: {
      int tmp_ret = OB_SUCCESS;
      // TMP_FAIL, if push seg file to remove task failed, the seg_id will be deleted by SSTmpFileARemove thread finally
      if (OB_TMP_FAIL(push_del_seg_file_to_remove_queue(seg_id_, del_seg_valid_len_))) {
        LOG_WARN("fail to push seg file to remove queue", KR(tmp_ret), K_(seg_id), K_(del_seg_valid_len));
      }
      break;
    }
    case ObSSTmpFileSegDeleteType::LOCAL_AND_REMOTE: {
      // use OB_FAIL. if delete seg file failed, then make this whole write io failed.
      // so as to make local cache tmp file is always newest, and we can read local file in any time.
      if (OB_FAIL(delete_local_seg_file(seg_id_))) {
        LOG_WARN("fail to delete local seg file", KR(ret), K_(seg_id));
      }
      int tmp_ret = OB_SUCCESS;
      // TMP_FAIL, if push seg file to remove task failed, the seg_id will be deleted by SSTmpFileARemove thread finally
      if (OB_TMP_FAIL(push_del_seg_file_to_remove_queue(seg_id_, del_seg_valid_len_))) {
        LOG_WARN("fail to push seg file to remove queue", KR(tmp_ret), K_(seg_id), K_(del_seg_valid_len));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected segment delete operation type", KR(ret), K_(seg_del_type));
      break;
    }
  }
  return ret;
}

int ObSSTmpFileIOCallback::delete_local_seg_file(const TmpFileSegId &del_seg_id)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else {
    MacroBlockId segment_file_id;
    segment_file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    segment_file_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
    segment_file_id.set_second_id(del_seg_id.tmp_file_id_);
    segment_file_id.set_third_id(del_seg_id.segment_id_);
    if (OB_FAIL(file_manager->delete_local_file(segment_file_id, 0/*ls_epoch_id*/,
                true/*is_print_log*/, false/*is_del_seg_meta*/))) { // delete old version seg_file, cannot delete seg_meta
      LOG_WARN("fail to delete local file", KR(ret), K(segment_file_id));
    }
  }
  return ret;
}

int ObSSTmpFileIOCallback::push_del_seg_file_to_remove_queue(const TmpFileSegId &del_seg_id,
                                                             const int64_t &del_seg_valid_len)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *file_manager = nullptr;
  if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file manager is null", KR(ret));
  } else if (OB_FAIL(file_manager->get_segment_file_mgr().push_seg_file_to_remove_queue(del_seg_id, del_seg_valid_len))) {
    LOG_WARN("fail to push seg file to remove queue", KR(ret), K(del_seg_id), K(del_seg_valid_len));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
