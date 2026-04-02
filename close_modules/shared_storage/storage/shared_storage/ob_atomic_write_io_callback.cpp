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

#include "storage/shared_storage/ob_atomic_write_io_callback.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "share/ob_ss_file_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

ObAtomicWriteIOCallback::ObAtomicWriteIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    ObIAllocator *allocator)
    : ObIOCallback(ObIOCallbackType::ATOMIC_WRITE_CALLBACK), macro_block_id_(macro_block_id),
      tenant_id_(tenant_id), ls_epoch_id_(ls_epoch_id), seq_(seq), offset_(offset), size_(size),
      allocator_(allocator)
{
}

int ObAtomicWriteIOCallback::rename_file()
{
  int ret = OB_SUCCESS;
  ObPathContext old_path_ctx;
  ObPathContext new_path_ctx;
  if (OB_FAIL(old_path_ctx.set_atomic_write_ctx(macro_block_id_, ls_epoch_id_, seq_))) {
    LOG_WARN("fail to construct old file path", KR(ret), KPC(this), K(old_path_ctx));
  } else if (OB_FAIL(new_path_ctx.set_file_ctx(macro_block_id_, ls_epoch_id_, true/*is_local_cache*/))) {
    LOG_WARN("fail to construct new file path", KR(ret), KPC(this), K(new_path_ctx));
  } else {
    int tmp_ret = OB_SUCCESS;
    RETRY_ON_EINTR(tmp_ret, ::rename(old_path_ctx.get_path(), new_path_ctx.get_path()));
    if (OB_UNLIKELY(0 != tmp_ret)) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_ERROR("fail to rename", KR(ret), K(errno), KERRMSG, K(old_path_ctx), K(new_path_ctx));
    }
  }

  // fail to rename file, try to delete file here
  if (OB_FAIL(ret) && (STRLEN(old_path_ctx.get_path()) > 0)) {
    int tmp_ret = OB_SUCCESS;
    if (0 != ::remove(old_path_ctx.get_path())) {
      tmp_ret = ObIODeviceLocalFileOp::convert_sys_errno();
      STORAGE_LOG(WARN, "fail to remove file", K(tmp_ret), K(errno), KERRMSG, K(old_path_ctx));
    }
  }
  return ret;
}

ObRenameIOCallback::ObRenameIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    ObIAllocator *allocator)
    : ObAtomicWriteIOCallback(macro_block_id, tenant_id, ls_epoch_id, seq, offset, size, allocator)
{
}

int ObRenameIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  UNUSED(data_buffer);
  UNUSED(size);
  int ret = OB_SUCCESS;
  if (OB_FAIL(rename_file())) {
    LOG_ERROR("fail to rename", KR(ret), KPC(this));
  }
  return ret;
}


ObRenameFsyncIOCallback::ObRenameFsyncIOCallback(
    const MacroBlockId &macro_block_id,
    const uint64_t tenant_id,
    const int64_t ls_epoch_id,
    const uint64_t seq,
    const int64_t offset,
    const int64_t size,
    ObIAllocator *allocator)
    : ObAtomicWriteIOCallback(macro_block_id, tenant_id, ls_epoch_id, seq, offset, size, allocator)
{
}

int ObRenameFsyncIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  UNUSED(data_buffer);
  UNUSED(size);
  int ret = OB_SUCCESS;
  char parent_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH];
  parent_dir_path[0] = '\0';
  if (OB_FAIL(rename_file())) {
    LOG_ERROR("fail to rename", KR(ret), KPC(this));
  } else if (OB_FAIL(ObFileHelper::get_file_parent_dir(parent_dir_path, sizeof(parent_dir_path),
                                                       macro_block_id_, ls_epoch_id_))) {
    LOG_ERROR("fail to get parent dir", KR(ret), KPC(this));
  } else if (OB_FAIL(ObDirManager::fsync_dir(parent_dir_path))) {
    LOG_ERROR("fail to fsync dir", KR(ret), K(parent_dir_path));
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
