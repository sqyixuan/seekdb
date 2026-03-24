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

#include "ob_local_cache_device.h"
#include "share/ob_resource_limit.h"
#include "share/ob_io_device_helper.h"
#include "storage/shared_storage/ob_disk_space_manager.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/**
 * ------------------------------------ObLocalCacheIOEvents--------------------------------------
 */
ObLocalCacheIOEvents::ObLocalCacheIOEvents()
  : complete_io_cnt_(0), io_events_(nullptr)
{
}

ObLocalCacheIOEvents::~ObLocalCacheIOEvents()
{
}

int64_t ObLocalCacheIOEvents::get_complete_cnt() const
{
  return complete_io_cnt_;
}

int ObLocalCacheIOEvents::get_ith_ret_code(const int64_t i) const
{
  int ret_code = -1;
  if (nullptr != io_events_ && i < complete_io_cnt_) {
    const int64_t res = static_cast<int64_t>(io_events_[i].res);
    if (res >= 0) {
      ret_code = 0;
    } else {
      ret_code = static_cast<int32_t>(-res);
    }
  } else {
    SHARE_LOG_RET(WARN, ret_code, "invalid member", KP(io_events_), K(i), K(complete_io_cnt_));
  }
  return ret_code;
}

int ObLocalCacheIOEvents::get_ith_ret_bytes(const int64_t i) const
{
  const int64_t res = static_cast<int64_t>(io_events_[i].res);
  return (nullptr != io_events_ && i < complete_io_cnt_ && res >= 0) ? static_cast<int32_t>(res) : 0;
}

void *ObLocalCacheIOEvents::get_ith_data(const int64_t i) const
{
  return (nullptr != io_events_ && i < complete_io_cnt_) ? io_events_[i].data : 0;
}


/**
 * ------------------------------------ObLocalCacheDevice---------------------------------------
 */
ObLocalCacheDevice::ObLocalCacheDevice()
  : is_inited_(false),
    data_disk_size_lock_(common::ObLatchIds::LOCAL_DEVICE_LOCK),
    block_size_(0),
    data_disk_size_(0),
    disk_percentage_(0),
    allocator_(),
    iocb_pool_()
{
  MEMSET(store_dir_, 0, sizeof(store_dir_));
  MEMSET(sstable_dir_, 0, sizeof(sstable_dir_));
}

ObLocalCacheDevice::~ObLocalCacheDevice()
{
  destroy();
}

int ObLocalCacheDevice::init(const ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "LoCacheDEVICE");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The local cache device has been inited", KR(ret));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),
                                     OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("Fail to init allocator", KR(ret));
  } else if (OB_FAIL(iocb_pool_.init(allocator_))) {
    LOG_WARN("Fail to init iocb pool", KR(ret));
  } else if (0 == opts.opt_cnt_) {
    LOG_INFO("For utl_file usage, skip initializing data disk size");
  } else {
    const char *store_dir = nullptr;
    const char *sstable_dir = nullptr;
    int64_t datafile_size = 0;
    int64_t block_size = 0;
    int64_t datafile_disk_percentage = 0;
    bool is_exist = false;
    int64_t media_id = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < opts.opt_cnt_; ++i) {
      if (0 == STRCMP(opts.opts_[i].key_, "data_dir")) {
        store_dir = opts.opts_[i].value_.value_str;
      } else if (0 == STRCMP(opts.opts_[i].key_, "sstable_dir")) {
        sstable_dir = opts.opts_[i].value_.value_str;
      } else if (0 == STRCMP(opts.opts_[i].key_, "block_size")) {
        block_size = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "datafile_disk_percentage")) {
        datafile_disk_percentage = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "datafile_size")) {
        datafile_size = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "media_id")) {
        media_id = opts.opts_[i].value_.value_int64;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Not supported option", KR(ret), K(i), K(opts.opts_[i].key_));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(store_dir) || 0 == STRLEN(store_dir)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid args", KR(ret), KP(store_dir));
      } else if (RL_IS_ENABLED && datafile_size > RL_CONF.get_max_datafile_size()) {
        ret = OB_RESOURCE_OUT;
        LOG_WARN("data disk size too large", KR(ret), K(datafile_size),
                 K(RL_CONF.get_max_datafile_size()));
      } else {
        block_size_ = block_size;
        data_disk_size_ = datafile_size;
        disk_percentage_ = datafile_disk_percentage;
        STRNCPY(store_dir_, store_dir, STRLEN(store_dir));
        STRNCPY(sstable_dir_, sstable_dir, STRLEN(sstable_dir));
        media_id_ = media_id;
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }

  return ret;
}

int ObLocalCacheDevice::reconfig(const ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObLocalCacheDevice has not been inited", KR(ret));
  } else {
    int64_t datafile_size = 0;
    int64_t datafile_disk_percentage = 0;
    int64_t reserved_size = 0;

    for (int64_t i = 0; OB_SUCC(ret) && i < opts.opt_cnt_; ++i) {
      if (0 == STRCMP(opts.opts_[i].key_, "datafile_disk_percentage")) {
        datafile_disk_percentage = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "datafile_size")) {
        datafile_size = opts.opts_[i].value_.value_int64;
      } else if (0 == STRCMP(opts.opts_[i].key_, "reserved_size")) {
        reserved_size = opts.opts_[i].value_.value_int64;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported option", KR(ret), K(i), K(opts.opts_[i].key_));
      }
    }

    if (OB_SUCC(ret)) {
      lib::ObMutexGuard guard(data_disk_size_lock_);
      int64_t adjust_disk_size = 0;
      // check disk space availability for new_data_disk_size_, must satisfy new_data_disk_size_ < used_disk_size + disk_free_space
      if (OB_FAIL(ObDiskSpaceManager::get_used_disk_size(adjust_disk_size))) {
        LOG_WARN("fail to get used disk size", KR(ret), K(adjust_disk_size));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::get_block_file_size(sstable_dir_, reserved_size, block_size_,
                 datafile_size, datafile_disk_percentage, adjust_disk_size))) {
        LOG_WARN("fail to get data disk size", KR(ret), K(reserved_size), K(block_size_),
                 K(datafile_size), K(datafile_disk_percentage));
      } else if (RL_IS_ENABLED && adjust_disk_size > RL_CONF.get_max_datafile_size()) {
        ret = OB_RESOURCE_OUT;
        LOG_WARN("data disk size too large", KR(ret), K(datafile_size), K(adjust_disk_size),
                 K(RL_CONF.get_max_datafile_size()));
      } else if (adjust_disk_size < data_disk_size_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can not make the data disk smaller", KR(ret), K(adjust_disk_size), K_(data_disk_size));
      } else if (adjust_disk_size == data_disk_size_) {
        LOG_INFO("the data disk size is not changed", K(adjust_disk_size), K_(data_disk_size));
      } else {
        data_disk_size_ = adjust_disk_size;
      }
    }
  }
  return ret;
}

int ObLocalCacheDevice::get_config(ObIODOpts &opts)
{
  UNUSED(opts);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::start(const ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the ObLocalCacheDevice has not been inited, ", KR(ret));
  } else if (1 != opts.opt_cnt_ || NULL == opts.opts_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("opts args is wrong ", KR(ret), K(opts.opt_cnt_));
  } else if (OB_UNLIKELY(0 != STRCMP(opts.opts_[0].key_, "reserved size")
                         || opts.opts_[0].value_.value_int64 < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid opt arguments", KR(ret), K(opts.opts_[0].key_),
             K(opts.opts_[0].value_.value_int64));
  } else {
    lib::ObMutexGuard guard(data_disk_size_lock_);
    const int64_t suggest_file_size = data_disk_size_;
    if (OB_FAIL(ObIODeviceLocalFileOp::compute_block_file_size(sstable_dir_, opts.opts_[0].value_.value_int64,
                block_size_, suggest_file_size, disk_percentage_, data_disk_size_))) {
      LOG_WARN("fail to get data disk size", KR(ret), K_(block_size), K_(data_disk_size),
               K(suggest_file_size), K_(disk_percentage));
    }
  }
  return ret;
}

void ObLocalCacheDevice::destroy()
{
  int ret = OB_SUCCESS;
  iocb_pool_.reset();
  allocator_.~ObFIFOAllocator();
  MEMSET(store_dir_, 0, sizeof(store_dir_));
  MEMSET(sstable_dir_, 0, sizeof(sstable_dir_));
  is_inited_ = false;
}

//file/dir interfaces
int ObLocalCacheDevice::open(
    const char *pathname,
    const int flags,
    const mode_t mode,
    ObIOFd &fd,
    ObIODOpts *opts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::open(pathname, flags, mode, fd, opts))) {
    LOG_WARN("Fail to open", KR(ret), K(pathname), K(flags), K(mode), K(fd));
  }
  return ret;
}

int ObLocalCacheDevice::complete(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::abort(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::close(fd))) {
    LOG_WARN("Fail to close", KR(ret), K(fd));
  }
  return ret;
}

int ObLocalCacheDevice::mkdir(const char *pathname, mode_t mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::mkdir(pathname, mode))) {
    LOG_WARN("Fail to mkdir", KR(ret), K(pathname), K(mode));
  }
  return ret;
}

int ObLocalCacheDevice::rmdir(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(pathname))) {
    LOG_WARN("Fail to rmdir", KR(ret), K(pathname));
  }
  return ret;
}

int ObLocalCacheDevice::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::unlink(pathname))) {
    LOG_WARN("Fail to unlink", KR(ret), K(pathname));
  }
  return ret;
}

int ObLocalCacheDevice::batch_del_files(
    const ObIArray<ObString> &files_to_delete, ObIArray<int64_t> &failed_files_idx)
{
  UNUSED(files_to_delete);
  UNUSED(failed_files_idx);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::rename(const char *oldpath, const char *newpath)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::rename(oldpath, newpath))) {
    LOG_WARN("Fail to rename", KR(ret), K(oldpath), K(newpath));
  }
  return ret;
}

int ObLocalCacheDevice::seal_file(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::seal_file(fd))) {
    LOG_WARN("Fail to seal_file", KR(ret), K(fd));
  }
  return ret;
}

int ObLocalCacheDevice::scan_dir(const char *dir_name, int (*func)(const dirent *entry))
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(dir_name, func))) {
    LOG_WARN("Fail to scan_dir", KR(ret), K(dir_name));
  }
  return ret;
}

int ObLocalCacheDevice::is_tagging(const char *pathname, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::is_tagging(pathname, is_tagging))) {
    LOG_WARN("Fail to check is_tagging", KR(ret), K(pathname));
  }
  return ret;
}

int ObLocalCacheDevice::scan_dir(const char *dir_name, ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::scan_dir(dir_name, op))) {
    LOG_WARN("Fail to scan_dir", KR(ret), K(dir_name));
  }
  return ret;
}

int ObLocalCacheDevice::fsync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fsync(fd))) {
    LOG_WARN("Fail to fsync", KR(ret), K(fd));
  }
  return ret;
}

int ObLocalCacheDevice::fdatasync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fdatasync(fd))) {
    LOG_WARN("Fail to fdatasync", KR(ret), K(fd));
  }
  return ret;
}

int ObLocalCacheDevice::fallocate(
    const ObIOFd &fd,
    mode_t mode,
    const int64_t offset,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fallocate(fd, mode, offset, len))) {
    LOG_WARN("Fail to fallocate", KR(ret), K(fd), K(mode), K(offset), K(len));
  }
  return ret;
}

int ObLocalCacheDevice::lseek(
    const ObIOFd &fd,
    const int64_t offset,
    const int whence,
    int64_t &result_offset)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::lseek(fd, offset, whence, result_offset))) {
    LOG_WARN("Fail to lseek", KR(ret), K(fd), K(offset), K(whence));
  }
  return ret;
}

int ObLocalCacheDevice::truncate(const char *pathname, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::truncate(pathname, len))) {
    LOG_WARN("Fail to truncate", KR(ret), K(pathname), K(len));
  }
  return ret;
}

int ObLocalCacheDevice::exist(const char *pathname, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::exist(pathname, is_exist))) {
    LOG_WARN("Fail to check is exist", KR(ret), K(pathname));
  }
  return ret;
}

int ObLocalCacheDevice::stat(const char *pathname, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::stat(pathname, statbuf))) {
    LOG_WARN("Fail to stat", KR(ret), K(pathname));
  }
  return ret;
}

int ObLocalCacheDevice::fstat(const ObIOFd &fd, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIODeviceLocalFileOp::fstat(fd, statbuf))) {
    LOG_WARN("Fail to fstat", KR(ret), K(fd));
  }
  return ret;
}

int ObLocalCacheDevice::del_unmerged_parts(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::adaptive_exist(const char *pathname, bool &is_exist)
{
  UNUSED(pathname);
  UNUSED(is_exist);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::adaptive_stat(const char *pathname, ObIODFileStat &statbuf)
{
  UNUSED(pathname);
  UNUSED(statbuf);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::adaptive_unlink(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op)
{
  UNUSED(dir_name);
  UNUSED(op);
  return OB_NOT_SUPPORTED;
}

// block interfaces
int ObLocalCacheDevice::mark_blocks(ObIBlockIterator &block_iter)
{
  UNUSED(block_iter);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::alloc_block(const ObIODOpts *opts, ObIOFd &block_id)
{
  UNUSED(opts);
  UNUSED(block_id);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::alloc_blocks(
    const ObIODOpts *opts,
    const int64_t count,
    ObIArray<ObIOFd> &blocks)
{
  UNUSED(opts);
  UNUSED(count);
  UNUSED(blocks);
  return OB_NOT_SUPPORTED;
}

void ObLocalCacheDevice::free_block(const ObIOFd &block_id)
{
  UNUSED(block_id);
}

int ObLocalCacheDevice::fsync_block()
{
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::get_restart_sequence(uint32_t &restart_id) const
{
  UNUSED(restart_id);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::mark_blocks(const ObIArray<ObIOFd> &blocks)
{
  UNUSED(blocks);
  return OB_NOT_SUPPORTED;
}

//sync io interfaces
int ObLocalCacheDevice::pread(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    ObIODPreadChecker *checker)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, not normal file", KR(ret), K(fd));
  } else {
    if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(fd.second_id_, buf, size, offset, read_size))) {
      LOG_WARN("failed to pread", KR(ret), K(fd), K(size), K(offset));
    }
    if (read_size < 0) {
      ret = OB_IO_ERROR;
      LOG_WARN("Fail to pread fd", KR(ret), K(read_size), K(offset), K(size), KP(buf), KERRMSG);
    }
    if (OB_SUCC(ret) && (nullptr != checker)) {
      if (OB_FAIL(checker->do_check(buf, read_size))) {
        LOG_WARN("check pread result fail", KR(ret), KP(buf), K(read_size));
      }
    }
  }
  return ret;
}

int ObLocalCacheDevice::pwrite(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    const void *buf,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, not normal file", KR(ret), K(fd));
  } else {
    if (OB_FAIL(ObIODeviceLocalFileOp::pwrite_impl(fd.second_id_, buf, size, offset, write_size))) {
      LOG_WARN("failed to pwrite", KR(ret), K(fd), K(size), K(offset));
    }
    if (write_size < 0) {
      ret = OB_IO_ERROR;
      LOG_WARN("Fail to pwrite fd", KR(ret), K(fd), K(write_size), K(offset), K(size),
                KP(buf), K(errno), KERRNOMSG(errno));
    }
  }
  return ret;
}

int ObLocalCacheDevice::read(
    const ObIOFd &fd,
    void *buf,
    const int64_t size,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::read(fd, buf, size, read_size))) {
    LOG_WARN("Fail to read", KR(ret), K(fd), K(buf), K(size));
  }
  return ret;
}

int ObLocalCacheDevice::write(
    const ObIOFd &fd,
    const void *buf,
    const int64_t size,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, buf, size, write_size))) {
    LOG_WARN("Fail to write", KR(ret), K(fd), K(buf), K(size));
  }
  return ret;
}

int ObLocalCacheDevice::upload_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const int64_t part_id,
    int64_t &write_size)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(part_id);
  UNUSED(write_size);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::buf_append_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const uint64_t tenant_id,
    bool &is_full)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(tenant_id);
  UNUSED(is_full);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::get_part_id(const ObIOFd &fd, bool &is_exist, int64_t &part_id)
{
  UNUSED(fd);
  UNUSED(is_exist);
  UNUSED(part_id);
  return OB_NOT_SUPPORTED;
}

int ObLocalCacheDevice::get_part_size(const ObIOFd &fd, const int64_t part_id, int64_t &part_size)
{
  UNUSED(fd);
  UNUSED(part_id);
  UNUSED(part_size);
  return OB_NOT_SUPPORTED;
}

//async io interfaces
int ObLocalCacheDevice::io_setup(
    uint32_t max_events,
    common::ObIOContext *&io_context)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLocalCacheIOContext)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory", KR(ret));
  } else {
    int sys_ret = 0;
    ObLocalCacheIOContext *local_cache_context = nullptr;
    local_cache_context = new (buf) ObLocalCacheIOContext();
    if (0 != (sys_ret = ::io_setup(max_events, &(local_cache_context->io_context_)))) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
      LOG_WARN("Fail to setup io context", KR(ret), K(sys_ret), KERRMSG);
    } else {
      io_context = local_cache_context;
    }
  }

  if (OB_FAIL(ret) && nullptr != buf) {
    allocator_.free(buf);
  }
  return ret;
}

int ObLocalCacheDevice::io_destroy(ObIOContext *io_context)
{
  int ret = OB_SUCCESS;
  ObLocalCacheIOContext *local_cache_io_context = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_ISNULL(io_context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KP(io_context));
  } else if (OB_UNLIKELY(ObIOContextType::IO_CONTEXT_TYPE_LOCAL_CACHE != io_context->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid io context pointer", KR(ret), KP(io_context),
             "io_context_type", io_context->get_type());
  } else if (OB_ISNULL(local_cache_io_context = static_cast<ObLocalCacheIOContext *>(io_context))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache io context is null", KR(ret), KP(io_context));
  } else {
    int sys_ret = 0;
    if ((sys_ret = ::io_destroy(local_cache_io_context->io_context_)) != 0) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
      LOG_WARN("Fail to destroy io context", KR(ret), K(sys_ret), KERRMSG);
    } else {
      allocator_.free(io_context);
    }
  }
  return ret;
}

int ObLocalCacheDevice::io_prepare_pwrite(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback)
{
  int ret = OB_SUCCESS;
  ObLocalCacheIOCB *local_cache_iocb = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_ISNULL(buf) || OB_ISNULL(iocb) || OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), KP(buf), KP(iocb), KP(callback));
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_LOCAL_CACHE != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_cache_iocb = static_cast<ObLocalCacheIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache iocb is null", KR(ret), KP(iocb));
  } else {
    ::io_prep_pwrite(&(local_cache_iocb->iocb_), static_cast<int32_t>(fd.second_id_), buf, count, offset);
    local_cache_iocb->iocb_.data = callback;
  }
  return ret;
}

int ObLocalCacheDevice::io_prepare_pread(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback)
{
  int ret = OB_SUCCESS;
  ObLocalCacheIOCB *local_cache_iocb = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_ISNULL(buf) || OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KR(ret), KP(buf), KP(iocb));
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_LOCAL_CACHE != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_cache_iocb = static_cast<ObLocalCacheIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache iocb is null", KR(ret), KP(iocb));
  } else {
    ::io_prep_pread(&(local_cache_iocb->iocb_), static_cast<int32_t>(fd.second_id_), buf, count, offset);
    local_cache_iocb->iocb_.data = callback;
  }

  return ret;
}

int ObLocalCacheDevice::io_submit(
    ObIOContext *io_context,
    ObIOCB *iocb)
{
  int ret = OB_SUCCESS;
  ObLocalCacheIOContext *local_cache_io_context = nullptr;
  ObLocalCacheIOCB *local_cache_iocb = nullptr;
  struct iocb *iocbp = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_ISNULL(io_context) || OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KP(io_context), KP(iocb));
  } else if (OB_UNLIKELY((ObIOContextType::IO_CONTEXT_TYPE_LOCAL_CACHE != io_context->get_type())
                         || (ObIOCBType::IOCB_TYPE_LOCAL_CACHE != iocb->get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid io_context or iocb pointer", KR(ret), KP(io_context), "io_context_type",
             io_context->get_type(), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_cache_io_context = static_cast<ObLocalCacheIOContext *>(io_context))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache io context pointer is null", KR(ret), KP(io_context));
  } else if (OB_ISNULL(local_cache_iocb = static_cast<ObLocalCacheIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache iocb pointer is null", KR(ret), KP(iocb));
  } else {
    iocbp = &(local_cache_iocb->iocb_);
    int submit_ret = ::io_submit(local_cache_io_context->io_context_, 1, &iocbp);
    if (1 != submit_ret) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-submit_ret);
      LOG_WARN("Fail to submit aio", KR(ret), K(submit_ret), K(errno), KERRMSG);
    }
  }
  return ret;
}

int ObLocalCacheDevice::io_cancel(
    ObIOContext *io_context,
    ObIOCB *iocb)
{
  int ret = OB_SUCCESS;
  ObLocalCacheIOContext *local_cache_io_context = nullptr;
  ObLocalCacheIOCB *local_cache_iocb = nullptr;
  struct io_event local_event;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_ISNULL(io_context) || OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KP(io_context), KP(iocb));
  } else if (OB_UNLIKELY((ObIOContextType::IO_CONTEXT_TYPE_LOCAL_CACHE != io_context->get_type())
                         || (ObIOCBType::IOCB_TYPE_LOCAL_CACHE != iocb->get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid io_context or iocb pointer", KR(ret), KP(io_context), "io_context_type",
             io_context->get_type(), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(local_cache_io_context = static_cast<ObLocalCacheIOContext *>(io_context))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache io context pointer is null", KR(ret), KP(io_context));
  } else if (OB_ISNULL(local_cache_iocb = static_cast<ObLocalCacheIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache iocb pointer is null", KR(ret), KP(iocb));
  } else {
    int sys_ret = 0;
    if ((sys_ret = ::io_cancel(local_cache_io_context->io_context_, &(local_cache_iocb->iocb_), &local_event)) < 0) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
      LOG_DEBUG("Fail to cancel aio", KR(ret), K(sys_ret), KERRMSG);
    }
  }
  return ret;
}

int ObLocalCacheDevice::io_getevents(
    ObIOContext *io_context,
    int64_t min_nr,
    ObIOEvents *events,
    struct timespec *timeout)
{
  int ret = OB_SUCCESS;
  ObLocalCacheIOContext *local_cache_io_context = nullptr;
  ObLocalCacheIOEvents *local_cache_io_events = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObLocalCacheDevice has not been inited", KR(ret));
  } else if (OB_ISNULL(io_context) || OB_ISNULL(events)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", KP(io_context), KP(events));
  } else if (OB_UNLIKELY((ObIOContextType::IO_CONTEXT_TYPE_LOCAL_CACHE != io_context->get_type())
                         || (ObIOEventsType::IO_EVENTS_TYPE_LOCAL_CACHE != events->get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid io_context or io_events pointer", KR(ret), KP(io_context), "io_context_type",
             io_context->get_type(), KP(events), "io_events_type", events->get_type());
  } else if (OB_ISNULL(local_cache_io_context = static_cast<ObLocalCacheIOContext *>(io_context))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache io context pointer is null", KR(ret), KP(io_context));
  } else if (OB_ISNULL(local_cache_io_events = static_cast<ObLocalCacheIOEvents *>(events))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local cache io events pointer is null", KR(ret), KP(events));
  } else {
    int sys_ret = 0;
    {
      oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_IO_EVENT);
      while ((sys_ret = ::io_getevents(
          local_cache_io_context->io_context_,
          min_nr,
          local_cache_io_events->max_event_cnt_,
          local_cache_io_events->io_events_,
          timeout)) < 0 && -EINTR == sys_ret); // ignore EINTR
    }
    if (sys_ret < 0) {
      // libaio on error it returns a negated error number (the negative of one of the values listed in ERRORS)
      ret = ObIODeviceLocalFileOp::convert_sys_errno(-sys_ret);
      LOG_WARN("Fail to get io events", KR(ret), K(sys_ret), KERRMSG);
    } else {
      local_cache_io_events->complete_io_cnt_ = sys_ret;
    }
  }
  return ret;
}

ObIOCB *ObLocalCacheDevice::alloc_iocb(const uint64_t tenant_id)
{
  UNUSED(tenant_id);
  ObLocalCacheIOCB *iocb = nullptr;
  ObLocalCacheIOCB *buf = nullptr;
  if (OB_LIKELY(is_inited_)) {
    if (NULL != (buf = iocb_pool_.alloc())) {
      iocb = new (buf) ObLocalCacheIOCB();
    }
  }
  return iocb;
}

ObIOEvents *ObLocalCacheDevice::alloc_io_events(const uint32_t max_events)
{
  ObLocalCacheIOEvents *io_events = nullptr;
  char *buf = nullptr;
  int64_t size = 0;

  if (OB_LIKELY(is_inited_)) {
    size = sizeof(ObLocalCacheIOEvents) + max_events * sizeof(struct io_event);
    if (NULL != (buf = (char*) allocator_.alloc(size))) {
      MEMSET(buf, 0, size);
      io_events = new (buf) ObLocalCacheIOEvents();
      io_events->max_event_cnt_ = max_events;
      io_events->complete_io_cnt_ = 0;
      io_events->io_events_ = reinterpret_cast<struct io_event *> (buf + sizeof(ObLocalCacheIOEvents));
    }
  }
  return io_events;
}

void ObLocalCacheDevice::free_iocb(ObIOCB *iocb)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    ObLocalCacheIOCB *local_cache_iocb = nullptr;
    if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_LOCAL_CACHE != iocb->get_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
    } else if (OB_ISNULL(local_cache_iocb = static_cast<ObLocalCacheIOCB *>(iocb))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local cache iocb is null", KR(ret), KP(iocb));
    } else {
      local_cache_iocb->~ObLocalCacheIOCB();
      iocb_pool_.free(local_cache_iocb);
    }
  }
}

void ObLocalCacheDevice::free_io_events(ObIOEvents *io_event)
{
  if (OB_LIKELY(is_inited_)) {
    allocator_.free(io_event);
  }
}

// space management interface
int64_t ObLocalCacheDevice::get_total_block_size() const
{
  return data_disk_size_;
}

int64_t ObLocalCacheDevice::get_free_block_count() const
{
  return 0;
}

int64_t ObLocalCacheDevice::get_reserved_block_count() const
{
  return 0;
}

int64_t ObLocalCacheDevice::get_max_block_count(int64_t reserved_size) const
{
  UNUSED(reserved_size);
  return 0;
}

int64_t ObLocalCacheDevice::get_max_block_size(int64_t reserved_size) const
{
  UNUSED(reserved_size);
  return 0;
}

int ObLocalCacheDevice::check_space_full(
    const int64_t required_size,
    const bool alarm_if_space_full) const
{
  UNUSED(required_size);
  UNUSED(alarm_if_space_full);
  return OB_SUCCESS;
}

int ObLocalCacheDevice::check_write_limited() const
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("check_write_limited is not support in local cache device !", KR(ret), K(device_type_));
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
