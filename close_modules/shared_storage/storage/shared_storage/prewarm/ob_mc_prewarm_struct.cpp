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

#include "ob_mc_prewarm_struct.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_handler.h"
#include "storage/shared_storage/prewarm/ob_ss_micro_cache_prewarm_service.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::compaction;
using namespace oceanbase::lib;
using namespace oceanbase::share;

/***************************ObHotMicroInfo**********************/
OB_SERIALIZE_MEMBER(ObHotMicroInfo, offset_, size_, logic_micro_id_, micro_crc_);

ObHotMicroInfo::ObHotMicroInfo() : offset_(0), size_(0), logic_micro_id_(), micro_crc_(0)
{
}

ObHotMicroInfo::ObHotMicroInfo(
    const int64_t offset,
    const int64_t size,
    const ObLogicMicroBlockId &logic_micro_id,
    const int64_t micro_crc)
  : offset_(offset), size_(size), logic_micro_id_(logic_micro_id), micro_crc_(micro_crc)
{
}


/***************************ObHotMacroInfo**********************/
OB_SERIALIZE_MEMBER(ObHotMacroInfo, macro_id_, hot_micro_infos_);

ObHotMacroInfo::ObHotMacroInfo() : macro_id_(), hot_micro_infos_()
{
  hot_micro_infos_.set_attr(ObMemAttr(MTL_ID(), "MC_PREWARM"));
}


/***************************ObHotTabletInfo**********************/
OB_SERIALIZE_MEMBER(ObHotTabletInfo, hot_macro_infos_);

ObHotTabletInfo::ObHotTabletInfo() : hot_macro_infos_()
{
  hot_macro_infos_.set_attr(ObMemAttr(MTL_ID(), "MC_PREWARM"));
}

void ObHotTabletInfo::reset()
{
  hot_macro_infos_.reuse();
}

int ObHotTabletInfo::push_back(
  const blocksstable::ObMicroBlockDesc &micro_block_desc,
  ObHotTabletInfoBaseWriter &writer)
{
  int ret = OB_SUCCESS;
  const int64_t macro_cnt = count();
  if (OB_ISNULL(micro_block_desc.header_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "micro block desc header is null", KR(ret), K(micro_block_desc));
  } else {
    ObHotMicroInfo micro_info(micro_block_desc.block_offset_,
        micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_,
        micro_block_desc.logic_micro_id_,
        (micro_block_desc.logic_micro_id_.is_valid() ? micro_block_desc.header_->data_checksum_ : 0));
    if ((macro_cnt > 0)
        && (micro_block_desc.macro_id_ == hot_macro_infos_.at(macro_cnt - 1).macro_id_)) {
      // TODO need to limit micro info count?
      if (OB_FAIL(hot_macro_infos_.at(macro_cnt - 1).push_back(micro_info))) {
        STORAGE_LOG(WARN, "failed to push back micro info", KR(ret), K(micro_info));
      }
    } else {
      ObHotMacroInfo macro_info;
      macro_info.macro_id_ = micro_block_desc.macro_id_;
      if (macro_cnt >= 8) {
        if (OB_FAIL(writer.append(hot_macro_infos_))) {
          STORAGE_LOG(WARN, "failed to write info", KR(ret), K(micro_info));
        } else {
          hot_macro_infos_.reuse();
        }
      }
      if (FAILEDx(macro_info.push_back(micro_info))) {
        STORAGE_LOG(WARN, "failed to push back micro info", KR(ret), K(micro_info));
      } else if (OB_FAIL(hot_macro_infos_.push_back(macro_info))) {
        STORAGE_LOG(WARN, "failed to push back macro info", KR(ret), K(micro_info));
      }
    }
  }
  return ret;
}

/***************************ObHotTabletInfoIndex**********************/
OB_SERIALIZE_MEMBER(ObHotTabletInfoIndex, sizes_);

ObHotTabletInfoIndex::ObHotTabletInfoIndex() : sizes_()
{
  sizes_.set_attr(ObMemAttr(MTL_ID(), "MC_PREWARM"));
}

void ObHotTabletInfoIndex::reset()
{
  sizes_.reuse();
}

int64_t ObHotTabletInfoIndex::get_total_size() const
{
  int64_t total_size = 0;
  const int64_t cnt = sizes_.count();
  for (int64_t i = 0; i < cnt; ++i) {
    total_size += sizes_.at(i);
  }
  return total_size;
}


/***************************ObHotTabletInfoBaseWriter**********************/
ObHotTabletInfoBaseWriter::ObHotTabletInfoBaseWriter(
    const int64_t tablet_id,
    const int64_t compaction_scn)
  : hot_tablet_info_(), hot_micro_cnt_(0), index_(), tablet_id_(tablet_id),
    compaction_scn_(compaction_scn), object_handle_(), last_serialize_size_(0),
    device_handle_(nullptr), fd_(), is_opened_(false), lock_(ObLatchIds::MC_PREWARM_LOCK)
{
}

ObHotTabletInfoBaseWriter::~ObHotTabletInfoBaseWriter()
{
  if (is_opened_ && OB_NOT_NULL(device_handle_)) {
    close_device_handle_and_fd();
  }
}

int ObHotTabletInfoBaseWriter::append(const ObIArray<ObHotMacroInfo> &hot_macro_infos)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  if (OB_FAIL(::append(hot_tablet_info_.hot_macro_infos_, hot_macro_infos))) {
    LOG_WARN("fail to append hot macro infos", KR(ret));
  } else {
    const int64_t hot_macro_info_cnt = hot_macro_infos.count();
    for (int64_t i = 0; (i < hot_macro_info_cnt); ++i) {
      hot_micro_cnt_ += hot_macro_infos.at(i).hot_micro_infos_.count();
    }
    if ((hot_micro_cnt_ >= FLUSH_THRESHOLD) && OB_FAIL(upload_hot_tablet_info())) {
      LOG_WARN("fail to upload hot tablet info", KR(ret));
    }
  }
  return ret;
}

int ObHotTabletInfoBaseWriter::complete()
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObHotTabletInfoBaseWriter::complete");
  ObBackupIoAdapter io_adapter;
  // if there exists residual hot tablet info, upload these residual hot tablet info
  if ((hot_micro_cnt_ > 0) && OB_FAIL(upload_hot_tablet_info())) {
    LOG_WARN("fail to upload hot tablet info", KR(ret));
  }
  time_guard.click("upload_hot_tablet_info");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(wait_if_need())) {
    LOG_WARN("fail to wait if need", KR(ret), K_(tablet_id), K_(compaction_scn));
  }
  time_guard.click("wait_if_need");
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(device_handle_)
             && (OB_FAIL(io_adapter.complete(*device_handle_, fd_)))) {
    LOG_WARN("fail to complete", KR(ret), KP_(device_handle), K_(fd));
  }
  time_guard.click("complete");
  if (OB_FAIL(ret) && OB_NOT_NULL(device_handle_)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(io_adapter.abort(*device_handle_, fd_))) {
      LOG_WARN("fail to abort", KR(tmp_ret), KP_(device_handle), K_(fd));
    }
  }
  time_guard.click("abort");
  // when hot tablet info does not exist, both prewarm_data and prewarm_index files are not uploaded.
  // when hot tablet info exist, both prewarm_data and prewarm_index files are uploaded.
  // here, if there exists hot tablet info index, upload these hot tablet info index.
  if (OB_SUCC(ret) && (index_.sizes_.count() > 0) && OB_FAIL(upload_hot_tablet_info_index())) {
    LOG_WARN("fail to upload hot tablet info index", KR(ret));
  }
  time_guard.click("upload_hot_tablet_info_index");
  close_device_handle_and_fd();
  time_guard.click("close_device_handle_and_fd");
  LOG_INFO("hot tablet info writer complete", KR(ret), K_(tablet_id), K_(compaction_scn), K(time_guard));
  return ret;
}

int ObHotTabletInfoBaseWriter::upload_hot_tablet_info()
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObHotTabletInfoBaseWriter::upload_hot_tablet_info");
  // if previous multipart upload failed, ignore ret and go on processing next multipart upload
  IGNORE_RETURN wait_if_need();
  time_guard.click("wait_if_need");

  ObBackupIoAdapter io_adapter;
  int64_t pos = 0;
  ObMemAttr attr(MTL_ID(), "MC_PREWARM");
  ObArenaAllocator allocator(attr);
  ObFileCommonHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t payload_length = hot_tablet_info_.get_serialize_size();
  const int64_t buf_len = header_length + payload_length;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(buf_len));
  } else if (OB_FAIL(ObFileCommonHeaderUtil::serialize<ObHotTabletInfo>(hot_tablet_info_,
                                             ObHotTabletInfo::OB_HOT_TABLET_INFO_VERSION,
                                             buf, buf_len, pos))) {
    LOG_WARN("fail to serialize hot tablet info", KR(ret), K_(hot_tablet_info), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(pos != buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pos", KR(ret), K(pos), K(buf_len));
  } else if (!is_opened_ && OB_FAIL(open_device_handle_and_fd())) {
    LOG_WARN("fail to open device handle and fd", KR(ret));
  } else if (!is_opened_ || OB_ISNULL(device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device handle is not opened or is null", KR(ret), K_(is_opened), KP_(device_handle));
  } else {
    const int64_t offset = index_.get_total_size();
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(get_data_object_type(), tablet_id_, compaction_scn_);
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle_))) {
      LOG_WARN("fail to alloc object", KR(ret));
    } else if (OB_FAIL(io_adapter.async_upload_data(*device_handle_, fd_, buf, offset, buf_len,
                                                    object_handle_.get_io_handle()))) {
      LOG_WARN("fail to async upload data", KR(ret), KP_(device_handle), K_(fd), KP(buf), K(offset), K(buf_len));
    } else {
      last_serialize_size_ = buf_len;
      // clear hot micro infos, which are already serialized into buf
      hot_tablet_info_.reset();
      hot_micro_cnt_ = 0;
    }
  }
  time_guard.click("async_upload_data");

  abort_on_fail(ret);
  time_guard.click("abort_on_fail");
  LOG_INFO("finish to upload hot tablet info", KR(ret), K_(tablet_id), K_(compaction_scn), K(buf_len), K(time_guard));
  return ret;
}

int ObHotTabletInfoBaseWriter::upload_hot_tablet_info_index()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMemAttr attr(MTL_ID(), "MC_PREWARM");
  ObArenaAllocator allocator(attr);
  ObFileCommonHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t payload_length = index_.get_serialize_size();
  const int64_t buf_len = header_length + payload_length;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(buf_len));
  } else if (OB_FAIL(ObFileCommonHeaderUtil::serialize<ObHotTabletInfoIndex>(index_,
                                             ObHotTabletInfoIndex::OB_HOT_TABLET_INFO_INDEX_VERSION,
                                             buf, buf_len, pos))) {
    LOG_WARN("fail to serialize hot tablet info", KR(ret), K_(index), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(pos != buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pos", KR(ret), K(pos), K(buf_len));
  } else {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = buf_len;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    ObSSObjectStorageWriter ss_object_storage_writer;
    ObStorageObjectHandle object_handle;
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(get_index_object_type(), tablet_id_, compaction_scn_);
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
      LOG_WARN("fail to alloc object", KR(ret), K(storage_opt));
    } else if (OB_FAIL(ss_object_storage_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("fail to wait", KR(ret));
    }
  }
  LOG_INFO("finish to upload hot tablet info index", KR(ret), K_(tablet_id), K_(compaction_scn), K(buf_len));
  return ret;
}

int ObHotTabletInfoBaseWriter::open_device_handle_and_fd()
{
  int ret = OB_SUCCESS;
  ObSSIOCommonOp ss_io_common_op;
  ObIOInfo io_info;
  io_info.tenant_id_ = MTL_ID();
  ObStorageObjectHandle object_handle;
  ObStorageObjectOpt storage_opt;
  storage_opt.set_ss_major_prewarm_opt(get_data_object_type(), tablet_id_, compaction_scn_);
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
    LOG_WARN("fail to alloc object", KR(ret));
  } else if (OB_FAIL(ss_io_common_op.get_object_device_and_fd(
                     ObStorageAccessType::OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER,
                     object_handle.get_macro_id(), 0/*ls_epoch_id*/, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), "macro_id", object_handle.get_macro_id());
  } else {
    device_handle_ = io_info.fd_.device_handle_;
    fd_ = io_info.fd_;
    is_opened_ = true;
  }
  return ret;
}

void ObHotTabletInfoBaseWriter::close_device_handle_and_fd()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_opened_)) {
    ObBackupIoAdapter io_adapter;
    if (OB_FAIL(io_adapter.close_device_and_fd(device_handle_, fd_))) {
      LOG_WARN("fail to close device and fd", KR(ret), KP_(device_handle), K_(fd));
    }
    device_handle_ = nullptr;
    fd_.reset();
    is_opened_ = false;
  }
}

int ObHotTabletInfoBaseWriter::wait_if_need()
{
  int ret = OB_SUCCESS;
  if (object_handle_.is_valid()) {
    // wait multipart upload
    if (OB_FAIL(object_handle_.wait())) {
      LOG_WARN("fail to wait", KR(ret), K_(tablet_id), K_(compaction_scn));
    } else if (OB_FAIL(index_.sizes_.push_back(last_serialize_size_))) {
      LOG_WARN("fail to push back", KR(ret), K_(last_serialize_size));
    } else {
      object_handle_.reset();
    }

    abort_on_fail(ret);
  }
  return ret;
}

void ObHotTabletInfoBaseWriter::abort_on_fail(const int ret)
{
  if (OB_SUCCESS != ret) {
    int tmp_ret = OB_SUCCESS;
    ObBackupIoAdapter io_adapter;
    if (OB_NOT_NULL(device_handle_) && OB_TMP_FAIL(io_adapter.abort(*device_handle_, fd_))) {
      LOG_WARN("fail to abort", KR(tmp_ret), KP_(device_handle), K_(fd));
    }
    close_device_handle_and_fd();
    index_.reset();
    object_handle_.reset();
  }
}


/***************************ObHotTabletInfoWriter**********************/
int ObHotTabletInfoWriter::complete()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(data_writer_.complete())) {
    ret = (OB_SUCC(ret) ? tmp_ret : ret);
    LOG_WARN("fail to complete data writer", KR(ret), KR(tmp_ret), K_(data_writer));
  }
  if (OB_TMP_FAIL(meta_writer_.complete())) {
    ret = (OB_SUCC(ret) ? tmp_ret : ret);
    LOG_WARN("fail to complete meta writer", KR(ret), KR(tmp_ret), K_(meta_writer));
  }
  return ret;
}


/***************************ObHotTabletInfoReader**********************/
ObHotTabletInfoReader::ObHotTabletInfoReader(
    const int64_t tablet_id,
    const int64_t compaction_scn,
    const int64_t prewarm_percent)
  : tablet_id_(tablet_id), compaction_scn_(compaction_scn), micro_buf_allocator_(),
    object_handles_(), is_aggregated_read_(), handle_idx_(-1), aggregated_read_parallelism_(1),
    aggregated_read_cnt_(0), independent_read_cnt_(0), prewarm_percent_(prewarm_percent),
    succ_add_micro_cnt_(0), fail_add_micro_cnt_(0), fail_wait_handle_cnt_(0),
    consecutive_succ_cnt_(0)
{
  micro_buf_allocator_.set_attr(ObMemAttr(MTL_ID(), "MC_PREWARM"));
}

ObHotTabletInfoReader::~ObHotTabletInfoReader()
{
  reset_io_resources();
}

int ObHotTabletInfoReader::load_hot_macro_infos()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  bool is_exist;
  ObHotTabletInfoIndex index;
  bool is_stop_prewarm = false;
  if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
    LOG_WARN("fail to check if stop prewarm", KR(ret));
  } else if (is_stop_prewarm) {
    LOG_INFO("stop prewarm", K_(tablet_id), K_(compaction_scn));
  } else if (OB_FAIL(load_hot_macro_infos(ObStorageObjectType::MAJOR_PREWARM_DATA,
                                          ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX))) {
    LOG_WARN("fail to load hot macro infos", KR(ret));
  } else if (OB_FAIL(load_hot_macro_infos(ObStorageObjectType::MAJOR_PREWARM_META,
                                          ObStorageObjectType::MAJOR_PREWARM_META_INDEX))) {
    LOG_WARN("fail to load hot macro infos", KR(ret));
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  LOG_INFO("finish to load hot macro infos", KR(ret), K(cost_us), K_(tablet_id), K_(compaction_scn),
           K_(succ_add_micro_cnt), K_(fail_add_micro_cnt), K_(fail_wait_handle_cnt));
  return ret;
}

int ObHotTabletInfoReader::load_hot_macro_infos(
    const ObStorageObjectType data_object_type,
    const ObStorageObjectType index_object_type)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObHotTabletInfoIndex index;
  if (OB_UNLIKELY(!(((ObStorageObjectType::MAJOR_PREWARM_DATA == data_object_type) &&
                    (ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX == index_object_type)) ||
                   ((ObStorageObjectType::MAJOR_PREWARM_META == data_object_type) &&
                    (ObStorageObjectType::MAJOR_PREWARM_META_INDEX == index_object_type))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid object type", KR(ret), K(data_object_type), "data_object_type_str",
             get_storage_objet_type_str(data_object_type), K(index_object_type),
             "index_object_type_str", get_storage_objet_type_str(index_object_type));
  } else if (OB_FAIL(check_if_index_file_exist(index_object_type, is_exist))) {
    LOG_WARN("fail to check if index file exist", KR(ret), K(index_object_type),
             "index_object_type_str", get_storage_objet_type_str(index_object_type));
  } else if (!is_exist) {
    // do nothing
    LOG_INFO("index file not exist", K_(tablet_id), K_(compaction_scn));
  } else if (OB_FAIL(read_index_file(index_object_type, index))) {
    LOG_WARN("fail to read index file", KR(ret), K_(tablet_id), K_(compaction_scn), K(index_object_type),
             "index_object_type_str", get_storage_objet_type_str(index_object_type));
  } else if (OB_FAIL(read_data_file(data_object_type, index))) {
    LOG_WARN("fail to read data file", KR(ret), K_(tablet_id), K_(compaction_scn), K(data_object_type),
             "data_object_type_str", get_storage_objet_type_str(data_object_type));
  }
  return ret;
}

bool ObHotTabletInfoReader::is_vaild_data_object_type(
    const ObStorageObjectType data_object_type) const
{
  return ((ObStorageObjectType::MAJOR_PREWARM_DATA == data_object_type) ||
          (ObStorageObjectType::MAJOR_PREWARM_META == data_object_type));
}

bool ObHotTabletInfoReader::is_vaild_index_object_type(
    const ObStorageObjectType index_object_type) const
{
  return ((ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX == index_object_type) ||
          (ObStorageObjectType::MAJOR_PREWARM_META_INDEX == index_object_type));
}

int ObHotTabletInfoReader::check_if_index_file_exist(
    const ObStorageObjectType object_type,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!is_vaild_index_object_type(object_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type));
  } else {
    ObStorageObjectHandle object_handle;
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(object_type, tablet_id_, compaction_scn_);
    ObTenantFileManager *file_manager = nullptr;
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
      LOG_WARN("fail to alloc object", KR(ret));
    } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret), "tenant_id", MTL_ID());
    } else if (OB_FAIL(file_manager->is_exist_file(object_handle.get_macro_id(), 0/*ls_epoch_id*/, is_exist))) {
      LOG_WARN("fail to check is exist file", KR(ret), "macro_id", object_handle.get_macro_id());
    }
  }
  return ret;
}

int ObHotTabletInfoReader::read_index_file(
    const ObStorageObjectType object_type,
    ObHotTabletInfoIndex &index)
{
  int ret = OB_SUCCESS;
  index.reset();
  char *buf = nullptr;
  ObMemAttr attr(MTL_ID(), "MC_PREWARM");
  ObArenaAllocator allocator(attr);
  ObStorageObjectHandle object_handle;
  ObStorageObjectOpt storage_opt;
  if (OB_UNLIKELY(!is_vaild_index_object_type(object_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type));
  } else if (FALSE_IT(storage_opt.set_ss_major_prewarm_opt(object_type, tablet_id_, compaction_scn_))) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(ObHotTabletInfoBaseWriter::INDEX_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), LITERAL_K(ObHotTabletInfoBaseWriter::INDEX_BUF_SIZE));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
    LOG_WARN("fail to alloc object", KR(ret), K(storage_opt));
  } else {
    int64_t pos = 0;
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = object_handle.get_macro_id();
    read_info.buf_ = buf;
    read_info.offset_ = 0;
    read_info.size_ = ObHotTabletInfoBaseWriter::INDEX_BUF_SIZE;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.mtl_tenant_id_ = MTL_ID();
    ObSSObjectStorageReader ss_object_storage_reader;
    if (OB_FAIL(ss_object_storage_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("fail to wait", KR(ret));
    } else if (OB_FAIL(ObFileCommonHeaderUtil::deserialize<ObHotTabletInfoIndex>(buf,
                       object_handle.get_data_size(), pos, index))) {
      LOG_WARN("fail to deserialize index", KR(ret), "data_size", object_handle.get_data_size(), K(pos));
    }
  }
  LOG_INFO("finish to read index file", KR(ret), K(object_type), "object_type_str",
           get_storage_objet_type_str(object_type), K_(tablet_id), K_(compaction_scn), K(index));
  return ret;
}

int ObHotTabletInfoReader::read_data_file(
    const ObStorageObjectType object_type,
    const ObHotTabletInfoIndex &index)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  int64_t cur_offset = 0;
  const int64_t cnt = index.sizes_.count();
  int64_t total_micro_cnt = 0;
  int64_t physic_micro_cnt = 0;
  int64_t logic_micro_cnt = 0;
  bool is_stop_prewarm = false;
  if (OB_UNLIKELY(!is_vaild_data_object_type(object_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type));
  }
  for (int64_t i = 0; (i < cnt) && !is_stop_prewarm && OB_SUCC(ret); ++i) {
    const int64_t size = index.sizes_.at(i);
    ObHotTabletInfo hot_tablet_info;
    char *buf = nullptr;
    ObMemAttr attr(MTL_ID(), "MC_PREWARM");
    ObArenaAllocator allocator(attr);
    ObStorageObjectHandle object_handle;
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(object_type, tablet_id_, compaction_scn_);
    if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
      LOG_WARN("fail to check if stop prewarm", KR(ret));
    } else if (is_stop_prewarm) {
      LOG_INFO("stop prewarm", K_(tablet_id), K_(compaction_scn));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(size));
    } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
      LOG_WARN("fail to alloc object", KR(ret), K(storage_opt));
    } else {
      int64_t pos = 0;
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = object_handle.get_macro_id();
      read_info.buf_ = buf;
      read_info.offset_ = cur_offset;
      read_info.size_ = size;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
      read_info.mtl_tenant_id_ = MTL_ID();
      ObSSObjectStorageReader ss_object_storage_reader;
      int64_t tmp_total_micro_cnt = 0;
      int64_t tmp_physic_micro_cnt = 0;
      int64_t tmp_logic_micro_cnt = 0;
      if (OB_FAIL(ss_object_storage_reader.aio_read(read_info, object_handle))) {
        LOG_WARN("fail to aio write", KR(ret), K(read_info), K(object_handle));
      } else if (OB_FAIL(object_handle.wait())) {
        LOG_WARN("fail to wait", KR(ret));
      } else if (OB_FAIL(ObFileCommonHeaderUtil::deserialize<ObHotTabletInfo>(buf, size, pos, hot_tablet_info))) {
        LOG_WARN("fail to deserialize hot tablet info", KR(ret), KP(buf), K(size), K(pos));
      } else if (OB_FAIL(load_hot_micro_block_data(hot_tablet_info, tmp_total_micro_cnt,
                                                   tmp_physic_micro_cnt, tmp_logic_micro_cnt))) {
        LOG_WARN("fail to load hot micro block data", KR(ret), K(hot_tablet_info));
      } else {
        total_micro_cnt += tmp_total_micro_cnt;
        physic_micro_cnt += tmp_physic_micro_cnt;
        logic_micro_cnt += tmp_logic_micro_cnt;
      }
    }
    cur_offset += size;
    ret = OB_SUCCESS; // ignore ret, so as to process next part of data_file
  }

  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  // print warning log when there are too much failed add micro block
  if (fail_add_micro_cnt_ > (total_micro_cnt / 10)) { // 10%
    FLOG_WARN("finish to read data file, too much failed add micro block", KR(ret), K(cost_us),
             K(object_type), "object_type_str", get_storage_objet_type_str(object_type),
             K_(tablet_id), K_(compaction_scn), K(total_micro_cnt), K(physic_micro_cnt),
             K(logic_micro_cnt), K_(prewarm_percent), K_(succ_add_micro_cnt), K_(fail_add_micro_cnt),
             K_(fail_wait_handle_cnt));
  } else {
    FLOG_INFO("finish to read data file", KR(ret), K(cost_us), K(object_type), "object_type_str",
              get_storage_objet_type_str(object_type), K_(tablet_id), K_(compaction_scn),
              K(total_micro_cnt), K(physic_micro_cnt), K(logic_micro_cnt), K_(prewarm_percent),
              K_(succ_add_micro_cnt), K_(fail_add_micro_cnt), K_(fail_wait_handle_cnt));
  }
  return ret;
}

int ObHotTabletInfoReader::load_hot_micro_block_data(
    const ObHotTabletInfo &hot_tablet_info,
    int64_t &total_micro_cnt,
    int64_t &physic_micro_cnt,
    int64_t &logic_micro_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObHotTabletInfo hot_tablet_info_to_prewarm;
  if (OB_FAIL(get_hot_tablet_info_to_prewarm(hot_tablet_info, hot_tablet_info_to_prewarm))) {
    LOG_WARN("fail to get hot tablet info to prewarm", KR(ret), K(hot_tablet_info));
  } else {
    const ObSEArray<ObHotMacroInfo, 8> &hot_macro_infos = hot_tablet_info_to_prewarm.hot_macro_infos_;
    const int64_t hot_macro_cnt = hot_macro_infos.count();
    for (int64_t i = 0; (i < hot_macro_cnt); ++i) {
      const ObSEArray<ObHotMicroInfo, 8> &cur_hot_micro_infos = hot_macro_infos.at(i).hot_micro_infos_;
      const int64_t cur_hot_micro_info_cnt = cur_hot_micro_infos.count();
      total_micro_cnt += cur_hot_micro_info_cnt;
      for (int64_t j = 0; (j < cur_hot_micro_info_cnt); ++j) {
        if (cur_hot_micro_infos.at(j).logic_micro_id_.is_valid()) {
          logic_micro_cnt++;
        } else {
          physic_micro_cnt++;
        }
      }
    }
  }
  const int64_t hot_macro_info_cnt = hot_tablet_info_to_prewarm.hot_macro_infos_.count();
  bool is_stop_prewarm = false;
  for (int64_t i = 0; (i < hot_macro_info_cnt) && !is_stop_prewarm && OB_SUCC(ret); ++i) {
    const ObHotMacroInfo &hot_macro_info = hot_tablet_info_to_prewarm.hot_macro_infos_.at(i);
    const int64_t hot_micro_info_cnt = hot_macro_info.hot_micro_infos_.count();
    if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
      LOG_WARN("fail to check if stop prewarm", KR(ret));
    } else if (is_stop_prewarm) {
      LOG_INFO("stop prewarm", K_(tablet_id), K_(compaction_scn));
    } else if (hot_micro_info_cnt >= AGGREGATED_READ_THRESHOLD) {
      // aggregate several small read into one large read
      if (OB_FAIL(aggregated_read(hot_macro_info))) {
        LOG_WARN("fail to aggregated read", KR(ret), K(hot_macro_info));
      }
    } else {
      for (int64_t j = 0; (j < hot_micro_info_cnt) && OB_SUCC(ret); ++j) {
        const ObHotMicroInfo &hot_micro_info = hot_macro_info.hot_micro_infos_.at(j);
        if (OB_FAIL(independent_read(hot_micro_info, hot_macro_info.macro_id_))) {
          LOG_WARN("fail to independent read", KR(ret), K(hot_micro_info), "macro_id",
                  hot_macro_info.macro_id_);
        }
        ret = OB_SUCCESS; // ignore ret, so as to process next hot micro info
      }
    }
    if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000L)) { // 1min
      FLOG_INFO("loading hot micro block data", KR(ret), K_(tablet_id), K_(compaction_scn),
               K(total_micro_cnt), K(physic_micro_cnt), K(logic_micro_cnt), K_(prewarm_percent),
               K_(aggregated_read_parallelism), K_(succ_add_micro_cnt), K_(fail_add_micro_cnt));
    }
    ret = OB_SUCCESS; // ignore ret, so as to process next hot macro info
  }

  // wait residual obejct handles
  if (OB_SUCC(ret)) {
    if ((handle_idx_ >= 0) && OB_FAIL(wait_all_object_handles())) {
      LOG_WARN("fail to wait all object handles", KR(ret), K_(handle_idx));
    }
  }
  return ret;
}

int ObHotTabletInfoReader::is_need_accumulate_size(
    const ObStorageObjectType object_type,
    bool &need_accumulate)
{
  int ret = OB_SUCCESS;
  need_accumulate = false;
  if (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type) {
    // do not accumulate meta macro
  } else if (ObStorageObjectType::SHARED_MAJOR_DATA_MACRO == object_type) {
    need_accumulate = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected object type", KR(ret), K(object_type), "object_type_str",
             get_storage_objet_type_str(object_type));
  }
  return ret;
}

int ObHotTabletInfoReader::check_if_inner_table_tablet(
    const ObHotTabletInfo &hot_tablet_info,
    bool &is_inner_table_tablet)
{
  int ret = OB_SUCCESS;
  is_inner_table_tablet = false;
  if (hot_tablet_info.hot_macro_infos_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hot_tablet_info));
  } else {
    const ObHotMacroInfo &hot_macro_info = hot_tablet_info.hot_macro_infos_.at(0);
    const ObTabletID tablet_id(hot_macro_info.macro_id_.second_id());
    if (tablet_id.is_inner_tablet()) {
      is_inner_table_tablet = true;
    }
  }
  return ret;
}

// return prewarm_percent * (prewarm micro size of SHARED_MAJOR_DATA_MACRO)
int ObHotTabletInfoReader::get_prewarm_size(
    const ObHotTabletInfo &hot_tablet_info,
    int64_t &prewarm_size)
{
  int ret = OB_SUCCESS;
  int64_t total_data_micro_size = 0;
  const int64_t hot_macro_info_cnt = hot_tablet_info.hot_macro_infos_.count();
  for (int64_t i = 0; (i < hot_macro_info_cnt) && OB_SUCC(ret); ++i) {
    const ObHotMacroInfo &hot_macro_info = hot_tablet_info.hot_macro_infos_.at(i);
    ObStorageObjectType object_type = hot_macro_info.macro_id_.storage_object_type();
    bool need_accumulate = false;
    if (OB_FAIL(is_need_accumulate_size(object_type, need_accumulate))) {
      LOG_WARN("fail to check if need accumulate size", KR(ret), K(object_type), "object_type_str",
               get_storage_objet_type_str(object_type));
    } else if (need_accumulate) {
      const int64_t hot_micro_info_cnt = hot_macro_info.hot_micro_infos_.count();
      for (int64_t j = 0; (j < hot_micro_info_cnt) && OB_SUCC(ret); ++j) {
        total_data_micro_size += hot_macro_info.hot_micro_infos_.at(j).size_;
      }
    }
  }

  if (OB_SUCC(ret)) {
    prewarm_size = total_data_micro_size * prewarm_percent_ / 100;
  }
  return ret;
}

int ObHotTabletInfoReader::get_hot_tablet_info_to_prewarm(
    const ObHotTabletInfo &in_hot_tablet_info,
    ObHotTabletInfo &out_hot_tablet_info)
{
  int ret = OB_SUCCESS;
  int64_t prewarm_size = 0;
  int64_t prewarmed_size = 0;
  bool is_inner_table_tablet = false;

  if (OB_FAIL(check_if_inner_table_tablet(in_hot_tablet_info, is_inner_table_tablet))) {
    LOG_WARN("fail to check if inner table tablet", KR(ret), K(in_hot_tablet_info));
  } else if (is_inner_table_tablet) {
    // prewarm inner table tablet anyway
    if (OB_FAIL(out_hot_tablet_info.hot_macro_infos_.assign(in_hot_tablet_info.hot_macro_infos_))) {
      LOG_WARN("fail to assign", KR(ret), K(in_hot_tablet_info));
    }
  } else { // !is_inner_table_tablet
    if (OB_FAIL(get_prewarm_size(in_hot_tablet_info, prewarm_size))) {
      LOG_WARN("fail to get prewarm size", KR(ret), K(in_hot_tablet_info));
    }
    // Note: prewarm_size only include SHARED_MAJOR_DATA_MACRO, not include SHARED_MAJOR_META_MACRO.
    // Even though prewarm_size == 0, need to prewarm SHARED_MAJOR_META_MACRO if exists.
    // Hence, for loop here cannot include prewarmed_size < prewarm_size condition.
    const int64_t hot_macro_info_cnt = in_hot_tablet_info.hot_macro_infos_.count();
    for (int64_t i = 0; (i < hot_macro_info_cnt) && OB_SUCC(ret); ++i) {
      const ObHotMacroInfo &hot_macro_info = in_hot_tablet_info.hot_macro_infos_.at(i);
      ObStorageObjectType object_type = hot_macro_info.macro_id_.storage_object_type();
      bool need_accumulate = false;
      if (OB_FAIL(is_need_accumulate_size(object_type, need_accumulate))) {
        LOG_WARN("fail to check if need accumulate size", KR(ret), K(object_type), "object_type_str",
                get_storage_objet_type_str(object_type));
      } else if (!need_accumulate) {
        if (OB_FAIL(out_hot_tablet_info.hot_macro_infos_.push_back(hot_macro_info))) {
          LOG_WARN("fail to push back", KR(ret), K(hot_macro_info));
        }
      } else if (prewarmed_size < prewarm_size) { // need_accumulate
        ObHotMacroInfo tmp_hot_macro_info;
        tmp_hot_macro_info.macro_id_ = hot_macro_info.macro_id_;
        const int64_t hot_micro_info_cnt = hot_macro_info.hot_micro_infos_.count();
        for (int64_t j = 0; (j < hot_micro_info_cnt) && (prewarmed_size < prewarm_size) && OB_SUCC(ret); ++j) {
          const ObHotMicroInfo &tmp_hot_micro_info = hot_macro_info.hot_micro_infos_.at(j);
          prewarmed_size += tmp_hot_micro_info.size_;
          if (OB_FAIL(tmp_hot_macro_info.push_back(tmp_hot_micro_info))) {
            LOG_WARN("fail to push back", KR(ret), K(tmp_hot_micro_info));
          }
        }
        if (FAILEDx(out_hot_tablet_info.hot_macro_infos_.push_back(tmp_hot_macro_info))) {
          LOG_WARN("fail to push back", KR(ret), K(tmp_hot_macro_info));
        }
      }
    }
  }
  return ret;
}

int ObHotTabletInfoReader::aggregated_read(
    const ObHotMacroInfo &hot_macro_info)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  int64_t size = 0;
  calc_offset_and_size(hot_macro_info, offset, size);
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(micro_buf_allocator_.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(size));
  } else {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = hot_macro_info.macro_id_;
    read_info.offset_ = offset;
    read_info.size_ = size;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);

    ObSSObjectStorageReader object_storage_reader;
    handle_idx_ = (handle_idx_ + 1) % TOTAL_READ_PARALLELISM;
    if (object_handles_[handle_idx_].is_valid()) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret), K_(handle_idx));
    } else if (FALSE_IT(is_aggregated_read_[handle_idx_] = true)) {
    } else if (FALSE_IT(++aggregated_read_cnt_)) {
    } else if (OB_FAIL(set_mc_prewarm_io_callback(hot_macro_info, offset, read_info))) {
      LOG_WARN("fail to set mc prewarm io callback", KR(ret), K(hot_macro_info), K(offset), K(read_info));
    } else if (OB_FAIL(object_storage_reader.aio_read(read_info, object_handles_[handle_idx_]))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info));
      if (OB_NOT_NULL(read_info.io_callback_)) {
        if ((ObIOCallbackType::SS_MC_PREWARM_CALLBACK == read_info.io_callback_->get_type())) {
          free_io_callback<ObSSMCPrewarmIOCallback>(read_info.io_callback_);
        } else {
          int tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid callback type", KR(tmp_ret), "callback_type", read_info.io_callback_->get_type());
        }
      }
    }

    if (OB_FAIL(ret)) {
      reset_io_resources();
    } else if ((aggregated_read_parallelism_ == aggregated_read_cnt_) &&
               OB_FAIL(wait_all_object_handles())) {
      LOG_WARN("fail to wait all object handles", KR(ret));
    }
  }
  return ret;
}

int ObHotTabletInfoReader::independent_read(
    const ObHotMicroInfo &hot_micro_info,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(micro_buf_allocator_.alloc(hot_micro_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), "size", hot_micro_info.size_);
  } else {
    ObIOInfo io_info;
    io_info.tenant_id_ = MTL_ID();
    io_info.offset_ = hot_micro_info.offset_;
    io_info.size_ = hot_micro_info.size_;
    io_info.flag_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    const int64_t real_timeout_ms = OB_IO_MANAGER.get_object_storage_io_timeout_ms(io_info.tenant_id_);
    io_info.timeout_us_ = real_timeout_ms * 1000L;
    io_info.user_data_buf_ = buf;

    ObSSMicroBlockCacheKey micro_key;
    if (hot_micro_info.logic_micro_id_.is_valid()) {
      micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
      micro_key.logic_micro_id_ = hot_micro_info.logic_micro_id_;
      micro_key.micro_crc_ = hot_micro_info.micro_crc_;
    } else {
      micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
      micro_key.micro_id_.macro_id_ = macro_id;
      micro_key.micro_id_.offset_ = io_info.offset_;
      micro_key.micro_id_.size_ = io_info.size_;
    }
    ObSSMicroBlockId phy_micro_id(macro_id, io_info.offset_, io_info.size_);
    ObSSMicroCache *micro_cache = nullptr;
    handle_idx_ = (handle_idx_ + 1) % TOTAL_READ_PARALLELISM;
    if (object_handles_[handle_idx_].is_valid()) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret), K_(handle_idx));
    } else if (FALSE_IT(is_aggregated_read_[handle_idx_] = false)) {
    } else if (FALSE_IT(++independent_read_cnt_)) {
    } else if (OB_UNLIKELY(!micro_key.is_valid() || (io_info.size_ <= 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(micro_key), "size", io_info.size_);
    } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro cache manager is null", KR(ret), "tenant_id", MTL_ID());
    } else if (OB_FAIL(micro_cache->get_micro_block_cache(micro_key, phy_micro_id,
               MicroCacheGetType::GET_CACHE_MISS_DATA,
               io_info, object_handles_[handle_idx_],
               ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE))) {
      LOG_WARN("fail to get micro block cache", KR(ret), K(micro_key), K(io_info));
    }

    if (OB_FAIL(ret)) {
      reset_io_resources();
    } else if ((INDEPENDENT_READ_PARALLELISM == independent_read_cnt_) &&
               OB_FAIL(wait_all_object_handles())) {
      LOG_WARN("fail to wait all object handles", KR(ret), K(micro_key));
    }
  }
  return ret;
}

void ObHotTabletInfoReader::calc_offset_and_size(
     const ObHotMacroInfo &hot_macro_info,
     int64_t &offset,
     int64_t &size)
{
  int64_t min_offset = INT64_MAX;
  int64_t max_offset = INT64_MIN;
  int64_t last_size = 0;
  const int64_t hot_micro_info_cnt = hot_macro_info.hot_micro_infos_.count();
  for (int64_t i = 0; i < hot_micro_info_cnt; ++i) {
    const ObHotMicroInfo &hot_micro_info = hot_macro_info.hot_micro_infos_.at(i);
    // update min_offset
    if (hot_micro_info.offset_ < min_offset) {
      min_offset = hot_micro_info.offset_;
    }
    // update max_offset and related size
    if (hot_micro_info.offset_ > max_offset) {
      max_offset = hot_micro_info.offset_;
      last_size = hot_micro_info.size_;
    }
  }

  // obtain final offset and size
  offset = min_offset;
  size = (max_offset - min_offset + last_size);
}

int ObHotTabletInfoReader::set_mc_prewarm_io_callback(
    const ObHotMacroInfo &hot_macro_info,
    const int64_t base_offset,
    ObStorageObjectReadInfo &read_info)
{
  int ret = OB_SUCCESS;
  ObIAllocator *callback_allocator = nullptr;
  ObSSMCPrewarmIOCallback *mc_prewarm_io_callback = nullptr;
  ObSSIOCommonOp ss_io_common_op;
  if (OB_FAIL(ss_io_common_op.get_io_callback_allocator(MTL_ID(), callback_allocator))) {
    LOG_WARN("fail to get io callback allocator", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_ISNULL(callback_allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback allocator is null", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_ISNULL(mc_prewarm_io_callback = static_cast<ObSSMCPrewarmIOCallback *>(
                          callback_allocator->alloc(sizeof(ObSSMCPrewarmIOCallback))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc cache load io callback memory", KR(ret), "size",
             sizeof(ObSSMCPrewarmIOCallback));
  } else {
    mc_prewarm_io_callback = new (mc_prewarm_io_callback) ObSSMCPrewarmIOCallback(
                                  callback_allocator, hot_macro_info, base_offset, read_info.buf_);
    read_info.io_callback_ = mc_prewarm_io_callback;
  }
  return ret;
}

int ObHotTabletInfoReader::wait_all_object_handles()
{
  int ret = OB_SUCCESS;
  ObSSMCPrewarmIOCallback *prewarm_io_callback[handle_idx_ + 1];
  for (int64_t i = 0; i <= handle_idx_; ++i) {
    prewarm_io_callback[i] = nullptr;
  }

  bool has_failed_agg_add = false;
  for (int64_t i = 0; (i <= handle_idx_) && OB_SUCC(ret); ++i) {
    if (object_handles_[i].is_empty()) {
      // do nothing.
    } else if (OB_FAIL(object_handles_[i].wait())) {
      // fail to wait. callback may be in callback queue, skip this case
      fail_wait_handle_cnt_++;
      LOG_WARN("fail to wait", KR(ret), K_(fail_wait_handle_cnt));
    } else if (is_aggregated_read_[i]) { // aggregated read
      ObSSMCPrewarmIOCallback *tmp_callback = nullptr;
      if (OB_FAIL(get_prewarm_io_callback(object_handles_[i], tmp_callback))) {
        LOG_WARN("fail to get prewarm io callback", KR(ret));
      } else if (OB_ISNULL(tmp_callback)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("callback is null", KR(ret));
      } else if (tmp_callback->get_failed_add_cnt() > 0) {
        prewarm_io_callback[i] = tmp_callback;
        has_failed_agg_add = true;
        succ_add_micro_cnt_ += (tmp_callback->hot_macro_info_.hot_micro_infos_.count()
                                - tmp_callback->get_failed_add_cnt());
      } else {
        succ_add_micro_cnt_ += tmp_callback->hot_macro_info_.hot_micro_infos_.count();
      }
    } else { // independent read
      // do not add again and adjust read parallelism for independent read
      ObSSMicroCacheIOCallback *tmp_callback = nullptr;
      if (OB_FAIL(get_micro_cache_io_callback(object_handles_[i], tmp_callback))) {
        LOG_WARN("fail to get micro cache io callback", KR(ret));
      } else if (OB_ISNULL(tmp_callback)) {
        succ_add_micro_cnt_++;
      } else if (tmp_callback->is_add_cache_failed()) {
        fail_add_micro_cnt_++;
      } else {
        succ_add_micro_cnt_++;
        if (OB_FAIL(try_move_meta_macro_into_t2(object_handles_[i], *tmp_callback))) {
          LOG_WARN("fail to try move meta macro into t2", KR(ret), "object_handle",
                   object_handles_[i], KPC(tmp_callback));
        }
      }
    }
    ret = OB_SUCCESS; // ignore ret, and process next handle
  }

  if (has_failed_agg_add) {
    consecutive_succ_cnt_ = 0;
    try_add_micro_block_cache_again(prewarm_io_callback);
    try_dec_agg_read_parallelism();
  } else {
    consecutive_succ_cnt_++;
    if (consecutive_succ_cnt_ > 10) {
      try_inc_agg_read_parallelism();
      consecutive_succ_cnt_ = 0;
    }
  }

  reset_io_resources();
  return ret;
}

int ObHotTabletInfoReader::get_prewarm_io_callback(
    ObStorageObjectHandle &object_handle,
    ObSSMCPrewarmIOCallback *&prewarm_io_callback)
{
  int ret = OB_SUCCESS;
  prewarm_io_callback = nullptr;
  ObIOCallback *io_callback = nullptr;
  if (OB_ISNULL(io_callback = object_handle.get_io_handle().get_io_callback())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io callback should not be null", KR(ret));
  } else if (ObIOCallbackType::SS_MC_PREWARM_CALLBACK == io_callback->get_type()) {
    prewarm_io_callback = static_cast<ObSSMCPrewarmIOCallback *>(io_callback);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected io callback type", KR(ret), "callback_type", io_callback->get_type());
  }
  return ret;
}

int ObHotTabletInfoReader::get_micro_cache_io_callback(
    ObStorageObjectHandle &object_handle,
    ObSSMicroCacheIOCallback *&micro_cache_io_callback)
{
  int ret = OB_SUCCESS;
  micro_cache_io_callback = nullptr;
  ObIOCallback *io_callback = nullptr;
  if (OB_ISNULL(io_callback = object_handle.get_io_handle().get_io_callback())) {
    // already in micro cache, and no need to add micro block cache again
  } else if ((ObIOCallbackType::SS_CACHE_LOAD_FROM_REMOTE_CALLBACK == io_callback->get_type()) ||
             (ObIOCallbackType::SS_CACHE_LOAD_FROM_LOCAL_CALLBACK == io_callback->get_type())) {
    micro_cache_io_callback = static_cast<ObSSMicroCacheIOCallback *>(io_callback);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected io callback type", KR(ret), "callback_type", io_callback->get_type());
  }
  return ret;
}

void ObHotTabletInfoReader::try_add_micro_block_cache_again(
    ObSSMCPrewarmIOCallback **prewarm_io_callback)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; (i <= handle_idx_) && OB_SUCC(ret); ++i) {
    if (OB_NOT_NULL(prewarm_io_callback[i])) {
      if (prewarm_io_callback[i]->can_retry_) {
        int64_t tmp_succ_add_cnt = 0;
        int64_t tmp_fail_add_cnt = 0;
        if (OB_FAIL(prewarm_io_callback[i]->add_micro_block_cache_again(tmp_succ_add_cnt, tmp_fail_add_cnt))) {
          LOG_WARN("fail to add micro block cache again", KR(ret), K(tmp_succ_add_cnt), K(tmp_fail_add_cnt));
        }
        succ_add_micro_cnt_ += tmp_succ_add_cnt;
        fail_add_micro_cnt_ += tmp_fail_add_cnt;
        if (tmp_fail_add_cnt > 0) {
          LOG_WARN("fail to add micro block cache until retry limit", KR(ret), K_(tablet_id), K_(compaction_scn),
                   K_(prewarm_percent), K_(aggregated_read_parallelism), K(tmp_fail_add_cnt));
        }
      } else { // !can_retry_
        fail_add_micro_cnt_ += prewarm_io_callback[i]->get_failed_add_cnt();
      }
    }
    ret = OB_SUCCESS; // ignore ret, process all prewarm io callback
  }
}

void ObHotTabletInfoReader::try_inc_agg_read_parallelism()
{
  const int64_t ori_agg_read_parallelism = aggregated_read_parallelism_;
  if (ori_agg_read_parallelism < AGGREGATED_READ_PARALLELISM_MAX) {
    if ((ori_agg_read_parallelism * 2) < AGGREGATED_READ_PARALLELISM_MAX) {
      aggregated_read_parallelism_ = ori_agg_read_parallelism * 2;
    } else {
      aggregated_read_parallelism_ = AGGREGATED_READ_PARALLELISM_MAX;
    }
    LOG_INFO("inc aggregated read parallelism", K_(tablet_id), K_(compaction_scn),
           K_(prewarm_percent), K_(succ_add_micro_cnt), K_(fail_add_micro_cnt),
           K(ori_agg_read_parallelism), K_(aggregated_read_parallelism));
  }
}

void ObHotTabletInfoReader::try_dec_agg_read_parallelism()
{
  const int64_t ori_agg_read_parallelism = aggregated_read_parallelism_;
  if (ori_agg_read_parallelism > AGGREGATED_READ_PARALLELISM_MIN) {
    aggregated_read_parallelism_ = ori_agg_read_parallelism * 3 / 4;
    LOG_INFO("dec aggregated read parallelism", K_(tablet_id), K_(compaction_scn),
           K_(prewarm_percent), K_(succ_add_micro_cnt), K_(fail_add_micro_cnt),
           K(ori_agg_read_parallelism), K_(aggregated_read_parallelism));
  }
}

int ObHotTabletInfoReader::check_if_stop_prewarm(bool &is_stop)
{
  int ret = OB_SUCCESS;
  is_stop = false;
  ObSSMicroCachePrewarmService *prewarm_service = nullptr;
  if (OB_ISNULL(prewarm_service = MTL(ObSSMicroCachePrewarmService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache prewarm service is null", KR(ret));
  } else if (ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL == prewarm_service->get_major_prewarm_level()) {
    is_stop = true;
  }
  return ret;
}

void ObHotTabletInfoReader::reset_io_resources()
{
  for (int64_t i = 0; i < TOTAL_READ_PARALLELISM; ++i) {
    object_handles_[i].reset();
    is_aggregated_read_[i] = false;
  }
  handle_idx_ = -1;
  aggregated_read_cnt_ = 0;
  independent_read_cnt_ = 0;
  micro_buf_allocator_.clear();
}

// in ARC algorithm of micro cache, micro blocks in T2 seg is hotter than those in T1 seg.
// we suppose meta macro is hotter than data macro, thus move meta macro into T2.
int ObHotTabletInfoReader::try_move_meta_macro_into_t2(
    const ObStorageObjectHandle &object_handle,
    ObSSMicroCacheIOCallback &micro_cache_io_callback)
{
  int ret = OB_SUCCESS;
  ObStorageObjectType object_type = object_handle.get_macro_id().storage_object_type();
  if ((ObIOCallbackType::SS_CACHE_LOAD_FROM_REMOTE_CALLBACK == micro_cache_io_callback.get_type()) &&
      (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type)) {
    ObSEArray<ObSSMicroBlockCacheKey, 1> micro_keys_to_add_t2;
    micro_keys_to_add_t2.set_attr(ObMemAttr(MTL_ID(), "MC_PREWARM"));
    ObSSMicroCache *micro_cache = nullptr;
    if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro cache manager is null", KR(ret), "tenant_id", MTL_ID());
    } else if (OB_FAIL(micro_keys_to_add_t2.push_back(micro_cache_io_callback.micro_key_))) {
      LOG_WARN("fail to push back", KR(ret), "micro_key", micro_cache_io_callback.micro_key_);
    } else if (OB_FAIL(micro_cache->update_micro_block_heat(micro_keys_to_add_t2,
                                    true/*need to transfer T1 -> T2*/,
                                    true/*need to update access_time to current_time*/))) {
      LOG_WARN("fail to update micro block heat", KR(ret), K(micro_keys_to_add_t2));
    }
  }
  return ret;
}


/***************************ObSSMCPrewarmIOCallback**********************/
ObSSMCPrewarmIOCallback::ObSSMCPrewarmIOCallback(
    ObIAllocator *allocator,
    const ObHotMacroInfo &hot_macro_info,
    const int64_t base_offset,
    char *user_data_buf)
  : ObIOCallback(ObIOCallbackType::SS_MC_PREWARM_CALLBACK), allocator_(allocator),
    hot_macro_info_(hot_macro_info), base_offset_(base_offset), user_data_buf_(user_data_buf),
    failed_idx_arr_(), can_retry_(true)
{
  failed_idx_arr_.set_attr(ObMemAttr(MTL_ID(), "MC_PREWARM"));
}

int ObSSMCPrewarmIOCallback::inner_process(const char *data_buffer, const int64_t size)
{
  int ret = OB_SUCCESS;
  const ObStorageObjectType object_type = hot_macro_info_.macro_id_.storage_object_type();
  const bool need_add_t2 = (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type);
  ObSEArray<ObSSMicroBlockCacheKey, 32> micro_keys_to_add_t2;
  micro_keys_to_add_t2.set_attr(ObMemAttr(MTL_ID(), "MC_PREWARM"));
  bool is_buffer_copied = false;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_UNLIKELY((nullptr == data_buffer) || (size <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", KR(ret), KP(data_buffer), K(size));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache manager is null", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_ISNULL(user_data_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user data buf is null", KR(ret));
  } else {
    // add each micro block into micro cache
    const int64_t hot_micro_info_cnt = hot_macro_info_.hot_micro_infos_.count();
    for (int64_t i = 0; (i < hot_micro_info_cnt) && OB_SUCC(ret); ++i) {
      const ObHotMicroInfo &hot_micro_info = hot_macro_info_.hot_micro_infos_.at(i);
      ObSSMicroBlockCacheKey micro_key;
      construct_micro_key(hot_micro_info, micro_key);
      if (OB_FAIL(micro_cache->add_micro_block_cache(micro_key,
                               // real offset of each micro block in data_buffer
                               data_buffer + hot_micro_info.offset_ - base_offset_,
                               hot_micro_info.size_,
                               ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE))) {
        LOG_WARN("fail to add micro block cache", KR(ret), K(micro_key));
        if (OB_EAGAIN != ret) { // Note: OB_EAGAIN means space is not enough now and can retry again
          can_retry_ = false;
        }
        if (!is_buffer_copied) {
          // fail to add micro block cache, copy buffer for add micro block cache again
          MEMCPY(user_data_buf_, data_buffer, size);
          is_buffer_copied = true;
        }
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(failed_idx_arr_.push_back(i))) {
          LOG_WARN("fail to push back", KR(tmp_ret), K(i));
        }
      } else if (need_add_t2 && OB_FAIL(micro_keys_to_add_t2.push_back(micro_key))) {
        LOG_WARN("fail to push back", KR(ret), K(micro_key));
      }
      ret = OB_SUCCESS; // ignore ret, and process next micro block
    }
    if (!micro_keys_to_add_t2.empty()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(micro_cache->update_micro_block_heat(micro_keys_to_add_t2,
                                                           true/*need to transfer T1 -> T2*/,
                                                           true/*need to update access_time to current_time*/))) {
        LOG_WARN("fail to update micro block heat", KR(tmp_ret), K(micro_keys_to_add_t2));
      }
    }
  }
  return ret;
}

int64_t ObSSMCPrewarmIOCallback::get_failed_add_cnt() const
{
  return failed_idx_arr_.count();
}

int ObSSMCPrewarmIOCallback::add_micro_block_cache_again(
    int64_t &succ_add_cnt,
    int64_t &fail_add_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t failed_idx_arr_cnt = failed_idx_arr_.count();
  succ_add_cnt = 0;
  const ObStorageObjectType object_type = hot_macro_info_.macro_id_.storage_object_type();
  const bool need_add_t2 = (ObStorageObjectType::SHARED_MAJOR_META_MACRO == object_type);
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache manager is null", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_ISNULL(user_data_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user data buf is null", KR(ret));
  } else if (OB_LIKELY(!failed_idx_arr_.empty())) {
    // micro blocks of each macro block max retry time
    const int64_t max_retry_time_us = 5 * 1000L * 1000L; // 5s
    const int64_t start_us = ObTimeUtility::fast_current_time();
    for (int64_t i = 0; OB_SUCC(ret) && (i < failed_idx_arr_cnt) &&
         ((ObTimeUtility::fast_current_time() - start_us) < max_retry_time_us); ++i) {
      const int64_t cur_failed_idx = failed_idx_arr_.at(i);
      const ObHotMicroInfo &hot_micro_info = hot_macro_info_.hot_micro_infos_.at(cur_failed_idx);
      ObSSMicroBlockCacheKey micro_key;
      construct_micro_key(hot_micro_info, micro_key);
      if (OB_FAIL(micro_cache->add_micro_block_cache_for_prewarm(micro_key,
                                   // real offset of each micro block in data_buffer
                                   user_data_buf_ + hot_micro_info.offset_ - base_offset_,
                                   hot_micro_info.size_,
                                   ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE,
                                   5/*max_retry_times*/, need_add_t2/*move from T1 to T2*/))) {
        LOG_WARN("fail to add micro block cache for prewarm", KR(ret), K(micro_key));
      } else {
        succ_add_cnt++;
      }
      ret = OB_SUCCESS; // ignore ret, and process next micro block
    }
  }
  fail_add_cnt = failed_idx_arr_cnt - succ_add_cnt;
  return ret;
}

void ObSSMCPrewarmIOCallback::construct_micro_key(
    const ObHotMicroInfo &hot_micro_info,
    ObSSMicroBlockCacheKey &micro_key) const
{
  if (hot_micro_info.logic_micro_id_.is_valid()) {
    micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
    micro_key.logic_micro_id_ = hot_micro_info.logic_micro_id_;
    micro_key.micro_crc_ = hot_micro_info.micro_crc_;
  } else {
    micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
    micro_key.micro_id_.macro_id_ = hot_macro_info_.macro_id_;
    micro_key.micro_id_.offset_ = hot_micro_info.offset_;
    micro_key.micro_id_.size_ = hot_micro_info.size_;
  }
}


/***************************ObMCPrewarmPercentUtil**********************/
int ObMCPrewarmPercentUtil::get_prewarm_percent(int64_t &prewarm_percent)
{
  int ret = OB_SUCCESS;
  static lib::ObMutex mutex;
  lib::ObMutexGuard guard(mutex);
  int64_t inner_table_micro_size = 0;
  int64_t tmp_prewarm_percent = -1;
  ObSSMicroCachePrewarmService *prewarm_service = MTL(ObSSMicroCachePrewarmService *);
  tmp_prewarm_percent = prewarm_service->get_major_prewarm_percent();
  if (-1 != tmp_prewarm_percent) { // already calculated
    prewarm_percent = tmp_prewarm_percent;
  } else { // calculate according to new micro info and micro cache size
    ObNewMicroInfo total_new_micro_info;
    ObArray<ObLSID> ls_arr;
    ObLSService *ls_service = MTL(ObLSService *);
    if (OB_FAIL(ls_service->get_ls_ids(ls_arr))) {
      LOG_WARN("fail to get all ls id", K(ret));
    } else {
      const int64_t ls_cnt = ls_arr.count();
      for (int64_t i = 0; (i < ls_cnt) && OB_SUCC(ret); ++i) {
        ObBasicObjHandle<ObLSObj> ls_obj_hdl;
        ObLSObj *ls_obj = nullptr;
        if (OB_FAIL(MTL_LS_OBJ_MGR.get_obj_handle(ls_arr.at(i), ls_obj_hdl, false/*add_if_not_exist*/))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // ignore ret
          } else {
            LOG_WARN("fail to get ls obj handle", K(ret), "ls_id", ls_arr.at(i));
          }
        } else if (OB_ISNULL(ls_obj = ls_obj_hdl.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls_obj is null", KR(ret), K(ls_obj_hdl));
        } else {
          ObNewMicroInfo tmp_new_micro_info;
          ls_obj->ls_compaction_status_.get_new_micro_info(tmp_new_micro_info);
          if (ls_arr.at(i).is_sys_ls()) { // inner table of sys ls
            inner_table_micro_size = tmp_new_micro_info.get_meta_micro_size()
                                     + tmp_new_micro_info.get_data_micro_size();
          } else {
            total_new_micro_info.add(tmp_new_micro_info);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t available_space_size = 0;
      ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
      if (OB_FAIL(micro_cache->get_available_space_for_prewarm(available_space_size))) {
        if (OB_SS_MICRO_CACHE_DISABLED == ret) {
          ret = OB_SUCCESS; // ignore ret
          available_space_size = 0;
          LOG_INFO("micro cache is disabled, set available space size to zero");
        } else {
          LOG_WARN("fail to get available space for prewarm", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t total_meta_micro_size = total_new_micro_info.get_meta_micro_size();
        const int64_t total_data_micro_size = total_new_micro_info.get_data_micro_size();
        prewarm_percent = MIN(100, MAX(0, available_space_size - inner_table_micro_size - total_meta_micro_size)
                                   * 1.0 / MAX(1, total_data_micro_size) * 100);
        prewarm_service->set_major_prewarm_percent(prewarm_percent);
        LOG_INFO("set prewarm percent", K(prewarm_percent), K(available_space_size), K(inner_table_micro_size), 
          K(total_meta_micro_size), K(total_data_micro_size));
      }
    }
  }
  return ret;
}


} /* namespace storage */
} /* namespace oceanbase */
