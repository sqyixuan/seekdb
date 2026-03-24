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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_HANDLER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_HANDLER_H_

#include <stdint.h>
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
  class ObStorageObjectHandle;
}
namespace storage
{

enum class MicroCacheGetType : uint8_t;

struct HandleHitDiskParam final
{
public:
  HandleHitDiskParam(const ObSSMicroBlockCacheKey &micro_key,
                     const blocksstable::MacroBlockId &macro_id,
                     const int64_t data_dest,
                     const uint32_t crc,
                     const MicroCacheGetType get_type,
                     const ObSSMicroCacheAccessType access_type);
  ~HandleHitDiskParam();
  TO_STRING_KV(K_(micro_key), K_(macro_id), K_(data_dest), K_(crc), K_(get_type), K_(access_type));

public:
  ObSSMicroBlockCacheKey micro_key_;
  blocksstable::MacroBlockId macro_id_;
  int64_t data_dest_;
  uint32_t crc_;
  MicroCacheGetType get_type_;
  ObSSMicroCacheAccessType access_type_;
};

class ObSSMicroCacheHandler : public ObSSIOCommonOp
{
public:
  int handle_not_hit(const ObSSMicroBlockCacheKey &micro_key,
                     const blocksstable::MacroBlockId &macro_id,
                     common::ObIOInfo &io_info,
                     blocksstable::ObStorageObjectHandle &object_handle,
                     const ObSSMicroCacheAccessType access_type);
  int handle_hit_memory(const MicroCacheGetType get_type,
                        const blocksstable::MacroBlockId &macro_id,
                        const char *io_buf,
                        common::ObIOInfo &io_info,
                        blocksstable::ObStorageObjectHandle &object_handle);
  int handle_hit_disk(const HandleHitDiskParam &param,
                      common::ObIOInfo &io_info,
                      blocksstable::ObStorageObjectHandle &object_handle);

private:
  int load_micro_data(const blocksstable::MacroBlockId &macro_id,
                      common::ObIOInfo &io_info);
  int load_from_local_cache_or_object_storage(const blocksstable::MacroBlockId &macro_id,
                                              common::ObIOInfo &io_info);
  int load_from_object_storage(const blocksstable::MacroBlockId &macro_id,
                               common::ObIOInfo &io_info);
  int set_cache_load_from_remote_io_callback(const ObSSMicroBlockCacheKey &micro_key,
                                             common::ObIOInfo &io_info,
                                             const ObSSMicroCacheAccessType access_type);
  int set_cache_load_from_local_io_callback(const ObSSMicroBlockCacheKey &micro_key,
                                            const uint32_t crc,
                                            common::ObIOInfo &io_info,
                                            const ObSSMicroCacheAccessType access_type);
  int set_macro_block_id(const MicroCacheGetType get_type,
                         const blocksstable::MacroBlockId &macro_id,
                         blocksstable::ObStorageObjectHandle &object_handle);
  void free_micro_cache_io_callback(common::ObIOCallback *&io_callback);
};

class ObSSMicroCacheIOCallback : public common::ObIOCallback
{
public:
  ObSSMicroCacheIOCallback(common::ObIAllocator *allocator, common::ObIOCallback *callback,
                           const ObSSMicroBlockCacheKey micro_key, char *user_data_buf, 
                           const ObSSMicroCacheAccessType access_type,
                           const common::ObIOCallbackType type)
    : common::ObIOCallback(type), allocator_(allocator), callback_(callback),
      micro_key_(micro_key), user_data_buf_(user_data_buf), access_type_(access_type),
      is_add_cache_failed_(false)
  {}

  virtual ~ObSSMicroCacheIOCallback() {}

  virtual const char *get_data() override
  {
    return (nullptr == callback_) ? user_data_buf_ : callback_->get_data();
  }

  virtual int64_t size() const override
  {
    return sizeof(*this);
  }

  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    return OB_NOT_SUPPORTED;
  }

  virtual int inner_process(const char *data_buffer, const int64_t size) = 0;

  virtual ObIAllocator *get_allocator() override { return allocator_; }
  // clear original io callback to avoid double free original io callback, because upper module is
  // responsible for free original io callback
  void clear_original_io_callback() { callback_ = nullptr; }

  OB_INLINE bool is_add_cache_failed() const { return ATOMIC_LOAD(&is_add_cache_failed_); }

  VIRTUAL_TO_STRING_KV(KP_(allocator), KP_(callback), K_(micro_key), KP_(user_data_buf),
                       K_(access_type), K_(is_add_cache_failed));

public:
  ObIAllocator *allocator_;
  ObIOCallback *callback_; // the original read io callback
  ObSSMicroBlockCacheKey micro_key_;
  char *user_data_buf_;
  ObSSMicroCacheAccessType access_type_;
  bool is_add_cache_failed_;
};

class ObSSCacheLoadFromRemoteIOCallback : public ObSSMicroCacheIOCallback
{
public:
  ObSSCacheLoadFromRemoteIOCallback(common::ObIAllocator *allocator, common::ObIOCallback *callback,
                                    const ObSSMicroBlockCacheKey micro_key, char *user_data_buf,
                                    const ObSSMicroCacheAccessType access_type)
    : ObSSMicroCacheIOCallback(allocator, callback, micro_key, user_data_buf, access_type,
                               common::ObIOCallbackType::SS_CACHE_LOAD_FROM_REMOTE_CALLBACK)
  {
  }

  virtual ~ObSSCacheLoadFromRemoteIOCallback();
  const char *get_cb_name() const override { return "SSCacheLoadFromRemoteIOCallback"; }

  virtual int inner_process(const char *data_buffer, const int64_t size) override;
};

class ObSSCacheLoadFromLocalIOCallback : public ObSSMicroCacheIOCallback
{
public:
  ObSSCacheLoadFromLocalIOCallback(common::ObIAllocator *allocator, common::ObIOCallback *callback,
                                   const ObSSMicroBlockCacheKey micro_key, char *user_data_buf,
                                   const ObSSMicroCacheAccessType access_type,
                                   const uint32_t crc)
    : ObSSMicroCacheIOCallback(allocator, callback, micro_key, user_data_buf, access_type,
                               common::ObIOCallbackType::SS_CACHE_LOAD_FROM_LOCAL_CALLBACK),
      crc_(crc)
  {
  }

  virtual ~ObSSCacheLoadFromLocalIOCallback();
  const char *get_cb_name() const override { return "SSCacheLoadFromLocalIOCallback"; }

  virtual int inner_process(const char *data_buffer, const int64_t size) override;

private:
  int check_crc(const char *data_buffer, const int64_t size);

public:
  uint32_t crc_;
};

template<typename T>
common::ObIOCallback *clear_original_io_callback(common::ObIOCallback *&io_callback)
{
  T *specific_callback = static_cast<T *>(io_callback);
  ObIOCallback *original_callback = specific_callback->callback_;
  // clear original io callback to avoid double free original io callback
  specific_callback->clear_original_io_callback();
  return original_callback;
}

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_HANDLER_H_ */
