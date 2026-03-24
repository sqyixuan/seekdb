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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_ATOMIC_WRITE_IO_CALLBACK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_ATOMIC_WRITE_IO_CALLBACK_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

/*
 * In order to implement atomic write on file system, designing ObAtomicWriteIOCallback
 * under the framework of ObIOManager::aio_write. The general idea is as follows:
 * 1. ObIOManager::aio_write one temporary file, i.e., /path/to/filename.tmp.seq.
 * 2. After all data is written, io_callback rename /path/to/filename.tmp.seq to /path/to/filename.
 * 3. After rename succ, io_callback fsync the parent dir of /path/to/filename, i.e., /path/to.
 */
class ObAtomicWriteIOCallback : public common::ObIOCallback
{
public:
  ObAtomicWriteIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                          const uint64_t tenant_id,
                          const int64_t ls_epoch_id,
                          const uint64_t seq,
                          const int64_t offset,
                          const int64_t size,
                          common::ObIAllocator *allocator);
  virtual ~ObAtomicWriteIOCallback() {}
  virtual ObIAllocator *get_allocator() override { return allocator_; }
  virtual const char *get_data() override { return nullptr; }
  virtual int64_t size() const override { return 0; }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_process(const char *data_buffer, const int64_t size) = 0;
  VIRTUAL_TO_STRING_KV(K_(macro_block_id), K_(tenant_id), K_(ls_epoch_id), K_(seq),
                       K_(offset), K_(size));

protected:
  // rename from /path/to/filename.tmp.seq to /path/to/filename
  int rename_file();

private:
  DISALLOW_COPY_AND_ASSIGN(ObAtomicWriteIOCallback);

public:
  blocksstable::MacroBlockId macro_block_id_;
  uint64_t tenant_id_;
  int64_t ls_epoch_id_;
  uint64_t seq_;
  int64_t offset_;
  int64_t size_;
  common::ObIAllocator *allocator_;
};

/*
 * design for files with PRIVATE_DATA_MACRO/PRIVATE_META_MACRO type.
 * need to rename, but no need to fsync parent dir.
 */
class ObRenameIOCallback : public ObAtomicWriteIOCallback
{
public:
  ObRenameIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                     const uint64_t tenant_id,
                     const int64_t ls_epoch_id,
                     const uint64_t seq,
                     const int64_t offset,
                     const int64_t size,
                     common::ObIAllocator *allocator);
  virtual ~ObRenameIOCallback() {}
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  const char *get_cb_name() const override { return "RenameIOCallback"; }
  VIRTUAL_TO_STRING_KV("callback_type:", "ObRenameIOCallback", K_(macro_block_id),
                       K_(tenant_id), K_(ls_epoch_id), K_(seq), K_(offset), K_(size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObRenameIOCallback);
};

/*
 * design for files with the following types:
 * LS_META, PRIVATE_TABLET_META, PRIVATE_TABLET_CURRENT_VERSION, LS_TRANSFER_TABLET_ID_ARRAY,
 * LS_ACTIVE_TABLET_ARRAY, LS_PENDING_FREE_TABLET_ARRAY, LS_DUP_TABLE_META,
 * SERVER_META, TENANT_SUPER_BLOCK and TENANT_UNIT_META.
 * need to rename and fsync parent dir.
 */
class ObRenameFsyncIOCallback : public ObAtomicWriteIOCallback
{
public:
  ObRenameFsyncIOCallback(const blocksstable::MacroBlockId &macro_block_id,
                          const uint64_t tenant_id,
                          const int64_t ls_epoch_id,
                          const uint64_t seq,
                          const int64_t offset,
                          const int64_t size,
                          common::ObIAllocator *allocator);
  virtual ~ObRenameFsyncIOCallback() {}
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  const char *get_cb_name() const override { return "RenameFsyncIOCallback"; }
  VIRTUAL_TO_STRING_KV("callback_type:", "ObRenameFsyncIOCallback", K_(macro_block_id),
                       K_(tenant_id), K_(ls_epoch_id), K_(seq), K_(offset), K_(size));

private:
  DISALLOW_COPY_AND_ASSIGN(ObRenameFsyncIOCallback);
};

enum class ObAtomicWriteIOCallbackType : uint8_t
{
  RENAME_TYPE = 0,
  RENAME_FSYNC_TYPE = 1,
  MAX_TYPE = 2
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_ATOMIC_WRITE_IO_CALLBACK_H_ */
