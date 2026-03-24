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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "common/storage/ob_io_device.h"
#include "common/ob_file_common_header.h"
#include "share/io/ob_io_define.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/ob_delegate.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObStorageObjectReadInfo;
struct ObMicroBlockDesc;
}
namespace storage
{
class ObHotTabletInfoBaseWriter;
class ObSSMCPrewarmIOCallback;
class ObSSMicroCacheIOCallback;
class ObHotMicroInfo
{
public:
  static const int64_t OB_HOT_MICRO_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_MICRO_INFO_VERSION);

public:
  ObHotMicroInfo();
  ObHotMicroInfo(const int64_t offset,
                 const int64_t size,
                 const blocksstable::ObLogicMicroBlockId &logic_micro_id,
                 const int64_t crc);
  virtual ~ObHotMicroInfo() {}
  TO_STRING_KV(K_(offset), K_(size), K_(logic_micro_id), K_(micro_crc));

public:
  int64_t offset_;
  int64_t size_;
  blocksstable::ObLogicMicroBlockId logic_micro_id_;
  int64_t micro_crc_;
};


class ObHotMacroInfo
{
public:
  static const int64_t OB_HOT_MACRO_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_MACRO_INFO_VERSION);

public:
  ObHotMacroInfo();
  virtual ~ObHotMacroInfo() {}
  CONST_DELEGATE_WITH_RET(hot_micro_infos_, count, int64_t);
  CONST_DELEGATE_WITH_RET(hot_micro_infos_, at, ObHotMacroInfo&);
  DELEGATE_WITH_RET(hot_micro_infos_, push_back, int);
  TO_STRING_KV(K_(macro_id), "hot_micro_info_cnt", hot_micro_infos_.count(), "hot_micro_infos",
               ObArrayWrap<ObHotMicroInfo>(hot_micro_infos_.get_data(),
               MIN(10, hot_micro_infos_.count())));

public:
  blocksstable::MacroBlockId macro_id_;
  common::ObSEArray<ObHotMicroInfo, 8> hot_micro_infos_;
};


class ObHotTabletInfo
{
public:
  static const int64_t OB_HOT_TABLET_INFO_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_TABLET_INFO_VERSION);

public:
  ObHotTabletInfo();
  virtual ~ObHotTabletInfo() {}
  void reset();
  CONST_DELEGATE_WITH_RET(hot_macro_infos_, count, int64_t);
  DELEGATE_WITH_RET(hot_macro_infos_, at, ObHotMacroInfo&);
  int push_back(const blocksstable::ObMicroBlockDesc &micro_block_desc, ObHotTabletInfoBaseWriter &writer);
  DELEGATE_WITH_RET(hot_macro_infos_, reuse, void);
  TO_STRING_KV("hot_macro_info_cnt", hot_macro_infos_.count(), "hot_macro_infos",
               ObArrayWrap<ObHotMacroInfo>(hot_macro_infos_.get_data(),
               MIN(10, hot_macro_infos_.count())));

public:
  common::ObSEArray<ObHotMacroInfo, 8> hot_macro_infos_;
};

class ObHotTabletInfoIndex
{
public:
  static const int64_t OB_HOT_TABLET_INFO_INDEX_VERSION = 1;
  OB_UNIS_VERSION(OB_HOT_TABLET_INFO_INDEX_VERSION);

public:
  ObHotTabletInfoIndex();
  virtual ~ObHotTabletInfoIndex() {}
  void reset();
  // sum of all recorded serialized sizes
  int64_t get_total_size() const;
  TO_STRING_KV("size_cnt", sizes_.count(), "sizes", ObArrayWrap<int64_t>(sizes_.get_data(),
               MIN(10, sizes_.count())));

public:
  // record serialized size of each multipart uploaded hot macro infos
  common::ObSEArray<int64_t, 8> sizes_;
};

// multipart upload data and put index
class ObHotTabletInfoBaseWriter
{
public:
  ObHotTabletInfoBaseWriter(const int64_t tablet_id, const int64_t compaction_scn);
  virtual ~ObHotTabletInfoBaseWriter();
  // Append hot macro infos. When accumulated 2w hot micro, serialize and upload the accumulated hot
  // macro infos to object storage, and record serialized size into ObHotTabletInfoIndex.
  // There exists concurrent call of this function, which requires mutex to ensure safety.
  int append(const common::ObIArray<ObHotMacroInfo> &hot_macro_infos);
  // Call this function after appending all hot macro infos, which will serialize and upload
  // the accumulated hot macro infos that has not yet been uploaded to object storage, and record
  // the serialized size in ObHotTabletInfoIndex. Besides, it will serialize and upload
  // ObHotTabletInfoIndex to object storage.
  int complete();
  VIRTUAL_TO_STRING_KV(K_(hot_tablet_info), K_(hot_micro_cnt), K_(index), K_(tablet_id),
                       K_(compaction_scn), K_(object_handle), KP_(device_handle), K_(fd), K_(is_opened));

private:
  int open_device_handle_and_fd();
  void close_device_handle_and_fd();
  int upload_hot_tablet_info();
  int upload_hot_tablet_info_index();
  int wait_if_need();
  void abort_on_fail(const int ret);
  virtual blocksstable::ObStorageObjectType get_data_object_type() const = 0;
  virtual blocksstable::ObStorageObjectType get_index_object_type() const = 0;

public:
  static const int64_t INDEX_BUF_SIZE = 1 * 1024 * 1024L; // 1MB

private:
  static const int64_t FLUSH_THRESHOLD = 20000L; // 2w
  ObHotTabletInfo hot_tablet_info_;
  int64_t hot_micro_cnt_;
  ObHotTabletInfoIndex index_;
  int64_t tablet_id_;
  int64_t compaction_scn_;
  // hold the io handle of the last multipart upload prewarm_data
  blocksstable::ObStorageObjectHandle object_handle_;
  // record the serialize size of the last multipart upload prewarm_data
  int64_t last_serialize_size_;
  common::ObIODevice *device_handle_;
  common::ObIOFd fd_;
  bool is_opened_;
  lib::ObMutex lock_;
};

// prewarm writer for SHARED_MAJOR_DATA_MACRO
class ObHotTabletInfoDataWriter : public ObHotTabletInfoBaseWriter
{
public:
  ObHotTabletInfoDataWriter(const int64_t tablet_id, const int64_t compaction_scn)
    : ObHotTabletInfoBaseWriter(tablet_id, compaction_scn)
  {}
  virtual ~ObHotTabletInfoDataWriter() {}

private:
  virtual blocksstable::ObStorageObjectType get_data_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA;
  }
  virtual blocksstable::ObStorageObjectType get_index_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX;
  }
};

// prewarm writer for SHARED_MAJOR_META_MACRO
class ObHotTabletInfoMetaWriter : public ObHotTabletInfoBaseWriter
{
public:
  ObHotTabletInfoMetaWriter(const int64_t tablet_id, const int64_t compaction_scn)
    : ObHotTabletInfoBaseWriter(tablet_id, compaction_scn)
  {}
  virtual ~ObHotTabletInfoMetaWriter() {}

private:
  virtual blocksstable::ObStorageObjectType get_data_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_META;
  }
  virtual blocksstable::ObStorageObjectType get_index_object_type() const override
  {
    return blocksstable::ObStorageObjectType::MAJOR_PREWARM_META_INDEX;
  }
};

class ObHotTabletInfoWriter final
{
public:
  ObHotTabletInfoWriter(const int64_t tablet_id, const int64_t compaction_scn)
    : data_writer_(tablet_id, compaction_scn), meta_writer_(tablet_id, compaction_scn)
  {}
  ~ObHotTabletInfoWriter() {}
  int complete();
  TO_STRING_KV(K_(data_writer), K_(meta_writer));

public:
  ObHotTabletInfoDataWriter data_writer_;
  ObHotTabletInfoMetaWriter meta_writer_;
};


// read index and data files, and then load hot micro block into micro cache
class ObHotTabletInfoReader
{
public:
  ObHotTabletInfoReader(const int64_t tablet_id,
                        const int64_t compaction_scn,
                        const int64_t prewarm_percent);
  virtual ~ObHotTabletInfoReader();
  int load_hot_macro_infos();
  TO_STRING_KV(K_(tablet_id), K_(compaction_scn), K_(object_handles), K_(handle_idx), K_(prewarm_percent));

private:
  bool is_vaild_data_object_type(const blocksstable::ObStorageObjectType data_object_type) const;
  bool is_vaild_index_object_type(const blocksstable::ObStorageObjectType index_object_type) const;
  int load_hot_macro_infos(const blocksstable::ObStorageObjectType data_object_type,
                           const blocksstable::ObStorageObjectType index_object_type);
  int check_if_index_file_exist(const blocksstable::ObStorageObjectType object_type, bool &is_exist);
  int read_index_file(const blocksstable::ObStorageObjectType object_type, ObHotTabletInfoIndex &index);
  int read_data_file(const blocksstable::ObStorageObjectType object_type, const ObHotTabletInfoIndex &index);
  int load_hot_micro_block_data(const ObHotTabletInfo &hot_tablet_info,
                                int64_t &total_micro_cnt,
                                int64_t &physic_micro_cnt,
                                int64_t &logic_micro_cnt);
  int is_need_accumulate_size(const blocksstable::ObStorageObjectType object_type, bool &need_accumulate);
  int check_if_inner_table_tablet(const ObHotTabletInfo &hot_tablet_info, bool &is_inner_table_tablet);
  int get_prewarm_size(const ObHotTabletInfo &hot_tablet_info, int64_t &prewarm_size);
  int get_hot_tablet_info_to_prewarm(const ObHotTabletInfo &in_hot_tablet_info,
                                     ObHotTabletInfo &out_hot_tablet_info);
  // design for macros with too many hot micros (e.g., more than 10 hot micros)
  int aggregated_read(const ObHotMacroInfo &hot_macro_info);
  // design for macros with too few hot micros (e.g., less than 10 hot micros)
  int independent_read(const ObHotMicroInfo &hot_micro_info,
                       const blocksstable::MacroBlockId &macro_id);
  // calculate read offset and read size for aggregated read
  void calc_offset_and_size(const ObHotMacroInfo &hot_macro_info, int64_t &offset, int64_t &size);
  int set_mc_prewarm_io_callback(const ObHotMacroInfo &hot_macro_info,
                                 const int64_t base_offset,
                                 blocksstable::ObStorageObjectReadInfo &read_info);
  int wait_all_object_handles();
  int get_prewarm_io_callback(blocksstable::ObStorageObjectHandle &object_handle,
                              ObSSMCPrewarmIOCallback *&prewarm_io_callback);
  int get_micro_cache_io_callback(blocksstable::ObStorageObjectHandle &object_handle,
                                  ObSSMicroCacheIOCallback *&micro_cache_io_callback);
  void try_add_micro_block_cache_again(ObSSMCPrewarmIOCallback **prewarm_io_callback);
  void try_inc_agg_read_parallelism();
  void try_dec_agg_read_parallelism();
  int check_if_stop_prewarm(bool &is_stop);
  void reset_io_resources();
  int try_move_meta_macro_into_t2(const blocksstable::ObStorageObjectHandle &object_handle,
                                  ObSSMicroCacheIOCallback &micro_cache_io_callback);

private:
  static const int64_t AGGREGATED_READ_THRESHOLD = 10;
  static const int64_t AGGREGATED_READ_PARALLELISM_MIN = 1;
  static const int64_t AGGREGATED_READ_PARALLELISM_MAX = 16;
  static const int64_t INDEPENDENT_READ_PARALLELISM = 100;
  static const int64_t TOTAL_READ_PARALLELISM = AGGREGATED_READ_PARALLELISM_MAX + INDEPENDENT_READ_PARALLELISM;
  int64_t tablet_id_;
  int64_t compaction_scn_;
  common::ObArenaAllocator micro_buf_allocator_;
  blocksstable::ObStorageObjectHandle object_handles_[TOTAL_READ_PARALLELISM];
  bool is_aggregated_read_[TOTAL_READ_PARALLELISM];
  int64_t handle_idx_;
  int64_t aggregated_read_parallelism_;
  int64_t aggregated_read_cnt_;
  int64_t independent_read_cnt_;
  int64_t prewarm_percent_;
  int64_t succ_add_micro_cnt_;
  int64_t fail_add_micro_cnt_;
  int64_t fail_wait_handle_cnt_;
  int64_t consecutive_succ_cnt_;
};

// design for aggregated object storage read of major compaction prewarm.
// it is responsible for adding each micro block in ObHotMacroInfo into micro cache.
class ObSSMCPrewarmIOCallback : public common::ObIOCallback
{
public:
  ObSSMCPrewarmIOCallback(common::ObIAllocator *allocator,
                          const ObHotMacroInfo &hot_macro_info,
                          const int64_t base_offset,
                          char *user_data_buf);
  virtual ~ObSSMCPrewarmIOCallback() {}
  virtual const char *get_data() override { return user_data_buf_; }
  virtual int64_t size() const override { return sizeof(*this); }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  virtual common::ObIAllocator *get_allocator() override { return allocator_; }
  int64_t get_failed_add_cnt() const;
  int add_micro_block_cache_again(int64_t &succ_add_cnt, int64_t &fail_add_cnt);
  const char *get_cb_name() const override { return "SSMCPrewarmIOCallback"; }
  TO_STRING_KV(KP_(allocator), K_(hot_macro_info), K_(base_offset), K_(failed_idx_arr),
               KP_(user_data_buf), K_(failed_idx_arr));

private:
  void construct_micro_key(const ObHotMicroInfo &hot_micro_info,
                           ObSSMicroBlockCacheKey &micro_key) const;

public:
  common::ObIAllocator *allocator_;
  ObHotMacroInfo hot_macro_info_;
  int64_t base_offset_; // aggregated read offset of macro block
  char *user_data_buf_;
  common::ObArray<int64_t> failed_idx_arr_;
  bool can_retry_;
};

struct ObMCPrewarmPercentUtil
{
  static int get_prewarm_percent(int64_t &prewarm_percent);
  static const int64_t DEFAULT_PREWARM_PERCENT = 10;
};



} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MC_PREWARM_STRUCT_H_ */
