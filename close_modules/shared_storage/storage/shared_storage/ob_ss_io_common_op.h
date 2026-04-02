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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_IO_COMMON_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_IO_COMMON_OP_H_

#include <stdint.h>
#include "common/storage/ob_io_device.h"
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/ob_file_helper.h"

namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
  enum class ObStorageObjectType : uint8_t;
}
namespace share
{
  class ObBackupDest;
}
namespace storage
{

struct TmpFileSegId;
struct TmpFileMeta;
class TmpFileMetaHandle;

class ObBaseFileManager;

class ObSSIOCommonOp
{
public:
  // get ObObjectDevice and simulated fd, and fill them into io_info
  int get_object_device_and_fd(const common::ObStorageAccessType access_type,
                               const blocksstable::MacroBlockId &macro_id,
                               const int64_t ls_epoch_id,
                               common::ObIOInfo &io_info) const;
  // construct storage_dest and storage_id with device_config obtained from ObDeviceConfigMgr
  int get_storage_dest_and_id(share::ObBackupDest &storage_dest, uint64_t &storage_id) const;
  // get object device from ObDeviceManager and open one simulated fd
  int open_object_fd(const ObIOInfo &io_info,
                     const blocksstable::MacroBlockId &macro_id,
                     const int64_t ls_epoch_id,
                     const share::ObBackupDest &storage_dest,
                     const uint64_t storage_id,
                     const common::ObStorageAccessType access_type,
                     common::ObIOFd &fd) const;
  // get ObLocalCacheDevice and fd of local cache read io, and fill them into io_info
  int get_local_cache_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                         const int64_t ls_epoch_id,
                                         common::ObIOInfo &io_info) const;
  static bool is_supported_fd_cache_obj_type(const blocksstable::ObStorageObjectType object_type);
  bool is_supported_access_type(const common::ObStorageAccessType access_type) const;
  int get_file_manager(const uint64_t tenant_id, ObBaseFileManager *&file_manager) const;
  int get_io_callback_allocator(const uint64_t tenant_id, common::ObIAllocator *&allocator) const;

protected:
  int get_fd_cache_handle(const char *file_path,
                          const int open_flag,
                          const blocksstable::MacroBlockId &macro_id,
                          ObBaseFileManager &file_manager,
                          common::ObIOInfo &io_info) const;
  int check_if_file_exist(const blocksstable::MacroBlockId &macro_id,
                          const int64_t ls_epoch_id,
                          bool &is_file_exist) const;
  int try_get_seg_meta(const TmpFileSegId &seg_id, TmpFileMetaHandle &meta_handle, bool &is_meta_exist) const;

private:
  bool need_get_from_fd_cache(const uint64_t tenant_id,
                              const blocksstable::ObStorageObjectType object_type) const;
  int get_from_fd_cache(const blocksstable::MacroBlockId &macro_id,
                        const int64_t ls_epoch_id,
                        common::ObIOInfo &io_info) const;
  int get_fd_by_open(const blocksstable::MacroBlockId &macro_id,
                     const int64_t ls_epoch_id,
                     common::ObIOInfo &io_info) const;
  int get_tmp_file_path(const common::ObIOInfo &io_info,
                        const blocksstable::MacroBlockId &macro_id,
                        ObPathContext &ctx) const;

protected:
  static constexpr int SS_DEFAULT_READ_FLAG = O_RDONLY | O_DIRECT;
  static constexpr int SS_DEFAULT_WRITE_FLAG = O_CREAT | O_EXCL | O_WRONLY | O_DIRECT | O_SYNC;
  static constexpr int SS_TMP_FILE_READ_FLAG = O_RDWR | O_DIRECT;
  static constexpr int SS_TMP_FILE_WRITE_FLAG = O_CREAT | O_RDWR | O_DIRECT;
  static constexpr mode_t SS_FILE_OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_IO_COMMON_OP_H_ */
