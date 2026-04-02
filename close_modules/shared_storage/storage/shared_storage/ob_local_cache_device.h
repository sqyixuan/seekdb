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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_LOCAL_CACHE_DEVICE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_LOCAL_CACHE_DEVICE_H_

#include <libaio.h>
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/lock/ob_mutex.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase
{
namespace storage
{

class ObLocalCacheDevice;

class ObLocalCacheIOCB : public common::ObIOCB
{
public:
  ObLocalCacheIOCB() : iocb_() {}
  virtual ~ObLocalCacheIOCB() {}
  virtual ObIOCBType get_type() const override
  {
    return ObIOCBType::IOCB_TYPE_LOCAL_CACHE;
  }
private:
  friend class ObLocalCacheDevice;
  struct iocb iocb_;
};

class ObLocalCacheIOContext : public common::ObIOContext
{
public:
  ObLocalCacheIOContext() : io_context_() {}
  virtual ~ObLocalCacheIOContext() {}
  virtual ObIOContextType get_type() const override
  {
    return ObIOContextType::IO_CONTEXT_TYPE_LOCAL_CACHE;
  }
private:
  friend class ObLocalCacheDevice;
  io_context_t io_context_;
};

class ObLocalCacheIOEvents : public common::ObIOEvents
{
public:
  ObLocalCacheIOEvents();
  virtual ~ObLocalCacheIOEvents();
  virtual ObIOEventsType get_type() const override
  {
    return ObIOEventsType::IO_EVENTS_TYPE_LOCAL_CACHE;
  }
  virtual int64_t get_complete_cnt() const override;
  virtual int get_ith_ret_code(const int64_t i) const override;
  virtual int get_ith_ret_bytes(const int64_t i) const override;
  virtual void *get_ith_data(const int64_t i) const override;
private:
  friend class ObLocalCacheDevice;
  int64_t complete_io_cnt_;
  struct io_event *io_events_;
};

class ObLocalCacheDevice : public common::ObIODevice
{
public:
  ObLocalCacheDevice();
  virtual ~ObLocalCacheDevice();
  virtual int init(const common::ObIODOpts &opts) override;
  virtual void destroy() override;
  virtual int reconfig(const common::ObIODOpts &opts) override;
  virtual int get_config(ObIODOpts &opts) override;

  virtual int start(const ObIODOpts &opts) override;

  //file/dir interfaces
  virtual int open(
    const char *pathname,
    const int flags,
    const mode_t mode,
    common::ObIOFd &fd,
    common::ObIODOpts *opts = NULL) override;
  virtual int complete(const ObIOFd &fd) override;
  virtual int abort(const ObIOFd &fd) override;
  virtual int close(const common::ObIOFd &fd) override;
  virtual int mkdir(const char *pathname, mode_t mode) override;
  virtual int rmdir(const char *pathname) override;
  virtual int unlink(const char *pathname) override;
  virtual int batch_del_files(
      const ObIArray<ObString> &files_to_delete, ObIArray<int64_t> &failed_files_idx) override;
  virtual int rename(const char *oldpath, const char *newpath) override;
  virtual int seal_file(const common::ObIOFd &fd) override;
  virtual int scan_dir(const char *dir_name, int (*func)(const dirent *entry)) override;
  virtual int scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op) override;
  virtual int is_tagging(const char *pathname, bool &is_tagging) override;
  virtual int fsync(const common::ObIOFd &fd) override;
  virtual int fdatasync(const common::ObIOFd &fd) override;
  virtual int fallocate(
    const common::ObIOFd &fd,
    mode_t mode,
    const int64_t offset,
    const int64_t len) override;
  virtual int lseek(
    const common::ObIOFd &fd,
    const int64_t offset,
    const int whence,
    int64_t &result_offset) override;
  virtual int truncate(const char *pathname, const int64_t len) override;
  virtual int exist(const char *pathname, bool &is_exist) override;
  virtual int stat(const char *pathname, common::ObIODFileStat &statbuf) override;
  virtual int fstat(const common::ObIOFd &fd, common::ObIODFileStat &statbuf) override;

  //for object device, local cache device should not use these
  virtual int del_unmerged_parts(const char *pathname) override;
  virtual int adaptive_exist(const char *pathname, bool &is_exist) override;
  virtual int adaptive_stat(const char *pathname, ObIODFileStat &statbuf) override;
  virtual int adaptive_unlink(const char *pathname) override;
  virtual int adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op) override;

  //block interfaces
  virtual int mark_blocks(common::ObIBlockIterator &block_iter) override;
  virtual int alloc_block(const common::ObIODOpts *opts, common::ObIOFd &block_id) override;
  virtual int alloc_blocks(
    const common::ObIODOpts *opts,
    const int64_t count,
    common::ObIArray<common::ObIOFd> &blocks) override;
  virtual void free_block(const common::ObIOFd &block_id) override;
  virtual int fsync_block() override;
  virtual int mark_blocks(const common::ObIArray<common::ObIOFd> &blocks) override;
  virtual int get_restart_sequence(uint32_t &restart_id) const override;

  //sync io interfaces
  virtual int pread(
    const common::ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    common::ObIODPreadChecker *checker = nullptr) override;
  virtual int pwrite(
    const common::ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    const void *buf,
    int64_t &write_size) override;
  virtual int read(
    const common::ObIOFd &fd,
    void *buf,
    const int64_t size,
    int64_t &read_size) override;
  virtual int write(
    const common::ObIOFd &fd,
    const void *buf,
    const int64_t size,
    int64_t &write_size) override;

  virtual int upload_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const int64_t part_id,
    int64_t &write_size) override;
  virtual int buf_append_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const uint64_t tenant_id,
    bool &is_full) override;
  virtual int get_part_id(const ObIOFd &fd, bool &is_exist, int64_t &part_id) override;
  virtual int get_part_size(const ObIOFd &fd, const int64_t part_id, int64_t &part_size) override;

  //async io interfaces
  virtual int io_setup(
    uint32_t max_events,
    common::ObIOContext *&io_context) override;
  virtual int io_destroy(common::ObIOContext *io_context) override;
  virtual int io_prepare_pwrite(
    const common::ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    common::ObIOCB *iocb,
    void *callback) override;
  virtual int io_prepare_pread(
    const common::ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    common::ObIOCB *iocb,
    void *callback) override;
  virtual int io_submit(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb) override;
  virtual int io_cancel(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb) override;
  virtual int io_getevents(
    common::ObIOContext *io_context,
    int64_t min_nr,
    common::ObIOEvents *events,
    struct timespec *timeout) override;
  virtual common::ObIOCB *alloc_iocb(const uint64_t tenant_id) override;
  virtual common::ObIOEvents *alloc_io_events(const uint32_t max_events) override;
  virtual void free_iocb(common::ObIOCB *iocb) override;
  virtual void free_io_events(common::ObIOEvents *io_event) override;

  // space management interface
  virtual int64_t get_total_block_size() const override;
  virtual int64_t get_free_block_count() const override;
  virtual int64_t get_max_block_size(int64_t reserved_size) const override;
  virtual int64_t get_max_block_count(int64_t reserved_size) const override;
  virtual int64_t get_reserved_block_count() const override;
  virtual int check_space_full(
    const int64_t required_size,
    const bool alarm_if_space_full = true) const override;
  virtual int check_write_limited() const override;

private:
  bool is_inited_;
  char store_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  char sstable_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  lib::ObMutex data_disk_size_lock_;
  int64_t block_size_;
  int64_t data_disk_size_;
  int64_t disk_percentage_;
  common::ObFIFOAllocator allocator_;
  ObIOCBPool<ObLocalCacheIOCB> iocb_pool_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_LOCAL_CACHE_DEVICE_H_ */
