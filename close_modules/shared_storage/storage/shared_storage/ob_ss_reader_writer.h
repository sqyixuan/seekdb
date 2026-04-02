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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_READER_WRITER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_READER_WRITER_H_

#include <stdint.h>
#include "common/storage/ob_io_device.h"
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/ob_atomic_write_io_callback.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"

namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
  class ObStorageObjectHandle;
  struct ObStorageObjectWriteInfo;
  struct ObStorageObjectReadInfo;
}
namespace storage
{

struct TmpFileSegId;
struct TmpFileMeta;

class ObSSBaseReader : public ObSSIOCommonOp
{
public:
  ObSSBaseReader() {}
  virtual ~ObSSBaseReader() {}
  int aio_read(const blocksstable::ObStorageObjectReadInfo &read_info,
               blocksstable::ObStorageObjectHandle &object_handle);

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info,
                                     const bool bypass_micro_cache,
                                     const bool is_major_macro_preread) = 0;
  virtual int read_from_local_cache_or_object_storage(const blocksstable::MacroBlockId &macro_id,
                                                      const int64_t ls_epoch_id,
                                                      common::ObIOInfo &io_info);
  virtual bool is_read_micro_cache(const int64_t offset) = 0;
  int read_micro_cache(const blocksstable::MacroBlockId &macro_id,
                       const blocksstable::ObLogicMicroBlockId &logic_micro_id,
                       const int64_t micro_crc,
                       common::ObIOInfo &io_info,
                       blocksstable::ObStorageObjectHandle &object_handle);
  // shared-storage mode write independent files which are 4KB alignment. however, upper modules
  // always read macros with size of 2MB, which leads to OB_DATA_OUT_RANGE errno. in order to avoid
  // OB_DATA_OUT_OF_RANGE errno, adjust upper modules' read size from 2MB to real-macro-file-size.
  // e.g., in shared-storage mode, there is one 100KB macro file, and upper modules try read 2MB.
  // need to adjust read size from 2MB to 100KB.
  // similarly, upper modules read PRIVATE_TABLET_META, SERVER_META, TENANT_SUPER_BLOCK,
  // TENANT_UNIT_META, LS_META... with a size larger than real size. io layer should adjust upper
  // modules' read size to real-file-size.
  int try_adjust_read_size(const blocksstable::MacroBlockId &macro_id,
                           const int64_t ls_epoch_id,
                           common::ObIOInfo &io_info);
  int read_file_from_read_cache(const blocksstable::MacroBlockId &macro_id,
                                const int64_t ls_epoch_id,
                                common::ObIOInfo &io_info,
                                const bool is_major_macro_preread);
  int push_lru_and_read_object_storage(const blocksstable::ObStorageObjectType object_type,
                                       const blocksstable::MacroBlockId &macro_id,
                                       const int64_t ls_epoch_id,
                                       common::ObIOInfo &io_info);
  int preread_next_segment_file(const blocksstable::MacroBlockId &file_id);
  bool is_need_preread(const blocksstable::ObStorageObjectType object_type,
                       const int64_t offset, const int64_t read_size);

private:
  int try_read_from_read_cache(const blocksstable::ObStorageObjectType object_type,
                               const blocksstable::MacroBlockId &macro_id,
                               const int64_t ls_epoch_id,
                               common::ObIOInfo &io_info);
  void record_local_cache_stat(const blocksstable::ObStorageObjectType object_type,
                               const int64_t delta_cnt, 
                               const int64_t delta_size,
                               const bool cache_hit);
};

class ObSSBaseWriter : public ObSSIOCommonOp
{
public:
  ObSSBaseWriter() {}
  virtual ~ObSSBaseWriter() {}
  int aio_write(const blocksstable::ObStorageObjectWriteInfo &write_info,
                blocksstable::ObStorageObjectHandle &object_handle);

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) = 0;
  virtual bool need_atomic_write() = 0;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) = 0;
  // get ObLocalCacheDevice and fd of local cache write io, and fill them into io_info
  int get_local_cache_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                          const int64_t ls_epoch_id,
                                          common::ObIOInfo &io_info);
  int set_atomic_write_io_callback(const blocksstable::MacroBlockId &macro_id,
                                   const int64_t ls_epoch_id,
                                   const uint64_t seq,
                                   common::ObIOInfo &io_info);
  int get_atomic_write_io_callback_type(const blocksstable::MacroBlockId &macro_id,
                                        ObAtomicWriteIOCallbackType &type);
  int alloc_atomic_write_io_callback(const ObAtomicWriteIOCallbackType &type,
                                     common::ObIAllocator &allocator,
                                     const blocksstable::MacroBlockId &macro_id,
                                     const uint64_t tenant_id,
                                     const int64_t ls_epoch_id,
                                     const uint64_t seq,
                                     const int64_t offset,
                                     const int64_t size,
                                     common::ObIOCallback *&callback);
  template <typename T>
  int create_atomic_write_io_callback(
      common::ObIAllocator &allocator,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t tenant_id,
      const int64_t ls_epoch_id,
      const uint64_t seq,
      const int64_t offset,
      const int64_t size,
      common::ObIOCallback *&callback)
  {
    int ret = OB_SUCCESS;
    static_assert(std::is_base_of<ObAtomicWriteIOCallback, T>::value,
                  "T must be a subclass of ObAtomicWriteIOCallback");
    T *io_callback = nullptr;
    if (OB_ISNULL(io_callback = static_cast<T *>(allocator.alloc(sizeof(T))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc atomic write io callback memory", KR(ret));
    } else {
      io_callback = new (io_callback) T(macro_id, tenant_id, ls_epoch_id, seq, offset, size, &allocator);
      callback = io_callback;
    }
    return ret;
  }
};


/* Reader and Writer of the following object types:
 * SHARED_MAJOR_TABLET_META, COMPACTION_SERVER, LS_SVR_COMPACTION_STATUS, COMPACTION_REPORT, LS_COMPACTION_STATUS, TABLET_COMPACTION_STATUS
 */
class ObSSObjectStorageReader : public ObSSBaseReader
{
public:
  ObSSObjectStorageReader() {}
  virtual ~ObSSObjectStorageReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info,
                                     const bool bypass_micro_cache,
                                     const bool is_major_macro_preread) override;
  virtual bool is_read_micro_cache(const int64_t offset) override;
};

class ObSSObjectStorageWriter : public ObSSBaseWriter
{
public:
  ObSSObjectStorageWriter() {}
  virtual ~ObSSObjectStorageWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
};

// Reader and Writer of the following object types:
// LS_META, LS_DUP_TABLE_META, LS_ACTIVE_TABLET_ARRAY, LS_PENDING_FREE_TABLET_ARRAY,
// LS_TRANSFER_TABLET_ID_ARRAY, PRIVATE_TABLET_META, PRIVATE_TABLET_CURRENT_VERSION,
// SERVER_META, TENANT_SUPER_BLOCK, TENANT_UNIT_META
class ObSSLocalCacheReader : public ObSSBaseReader
{
public:
  ObSSLocalCacheReader() {}
  virtual ~ObSSLocalCacheReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info,
                                     const bool bypass_micro_cache,
                                     const bool is_major_macro_preread) override;
  virtual bool is_read_micro_cache(const int64_t offset) override;
};

class ObSSLocalCacheWriter : public ObSSBaseWriter
{
public:
  ObSSLocalCacheWriter() {}
  virtual ~ObSSLocalCacheWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
};

// Reader and Writer of PRIVATE_DATA_MACRO/PRIVATE_META_MACRO object type
class ObSSPrivateMacroReader : public ObSSBaseReader
{
public:
  ObSSPrivateMacroReader() {}
  virtual ~ObSSPrivateMacroReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info,
                                     const bool bypass_micro_cache,
                                     const bool is_major_macro_preread) override;
  virtual int read_from_local_cache_or_object_storage(const blocksstable::MacroBlockId &macro_id,
                                                      const int64_t ls_epoch_id,
                                                      common::ObIOInfo &io_info) override;
  virtual bool is_read_micro_cache(const int64_t offset) override;
};

class ObSSPrivateMacroWriter : public ObSSBaseWriter
{
public:
  ObSSPrivateMacroWriter() {}
  virtual ~ObSSPrivateMacroWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
};

// Reader and Writer of SHARED_MAJOR_DATA_MACRO/SHARED_MAJOR_META_MACRO object type
class ObSSShareMacroReader : public ObSSBaseReader
{
public:
  ObSSShareMacroReader() {}
  virtual ~ObSSShareMacroReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info,
                                     const bool bypass_micro_cache,
                                     const bool is_major_macro_preread) override;
  virtual int read_from_local_cache_or_object_storage(const blocksstable::MacroBlockId &macro_id,
                                                      const int64_t ls_epoch_id,
                                                      common::ObIOInfo &io_info) override;
  virtual bool is_read_micro_cache(const int64_t offset) override;
};

class ObSSShareMacroWriter : public ObSSBaseWriter
{
public:
  ObSSShareMacroWriter() {}
  virtual ~ObSSShareMacroWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
};

// Reader and Writer of TMP_FILE object type
class ObSSTmpFileReader : public ObSSBaseReader
{
public:
  ObSSTmpFileReader() {}
  virtual ~ObSSTmpFileReader() {}

protected:
  virtual int get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                     const int64_t ls_epoch_id,
                                     common::ObIOInfo &io_info,
                                     const bool bypass_micro_cache,
                                     const bool is_major_macro_preread) override;
  virtual bool is_read_micro_cache(const int64_t offset) override;

private:
  int handle_read_of_meta_exist(const TmpFileMetaHandle &meta_handle,
                                const blocksstable::MacroBlockId &macro_id,
                                const int64_t ls_epoch_id,
                                common::ObIOInfo &io_info);
  int handle_read_of_meta_not_exist(const blocksstable::MacroBlockId &macro_id,
                                    const int64_t ls_epoch_id,
                                    common::ObIOInfo &io_info);
  int inner_get_read_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                   const int64_t ls_epoch_id,
                                   common::ObIOInfo &io_info);
};

class ObSSTmpFileWriter : public ObSSBaseWriter
{
public:
  ObSSTmpFileWriter() {}
  virtual ~ObSSTmpFileWriter() {}

protected:
  virtual int get_write_device_and_fd(const blocksstable::MacroBlockId &macro_id,
                                      const int64_t ls_epoch_id,
                                      common::ObIOInfo &io_info) override;
  virtual bool need_atomic_write() override;
  virtual int get_atomic_write_seq(const blocksstable::MacroBlockId &macro_id, uint64_t &seq) override;
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_READER_WRITER_H_ */
