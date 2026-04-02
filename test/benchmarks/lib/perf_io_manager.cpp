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

#include <gtest/gtest.h>
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"
#include "lib/file/file_directory_utils.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "share/config/ob_server_config.h"

using namespace obsys;

namespace oceanbase
{
namespace common
{

class TestIOManager : public ::testing::Test
{
public:
  TestIOManager();
  virtual ~TestIOManager();
  virtual void SetUp();
  virtual void TearDown();
protected:
  int fd_;
  char filename_[128];
  int64_t file_size_;
};


TestIOManager::TestIOManager()
  : fd_(0)
{
  file_size_ = 1024L * 1024L * 1024L * 20;
  snprintf(filename_, 128, "./io_mgr_test_file");
}

TestIOManager::~TestIOManager()
{
}

void TestIOManager::SetUp()
{
  int ret = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());

  FileDirectoryUtils::delete_file(filename_);
  ret = ObIOManager::get_instance().init(1024L * 1024L * 1024L, 10);
  ASSERT_EQ(OB_SUCCESS, ret);

  fd_ = ::open(filename_, O_CREAT | O_TRUNC | O_RDWR | O_DIRECT,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
  ASSERT_TRUE(fd_ > 0);
#ifdef HAVE_FALLOCATE
  ret = fallocate(fd_, 0/*MODE*/,0/*offset*/, file_size_);
#else
  ret = ftruncate(fd_, file_size_);
#endif
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestIOManager::TearDown()
{
  ObIOManager::get_instance().destroy();
  ::close(fd_);
  FileDirectoryUtils::delete_file(filename_);
}

class TestIOStress: public share::ObThreadPool
{
public:
  TestIOStress();
  virtual ~TestIOStress();
  void init(
    int fd,
    int64_t file_size,
    int32_t get_thread_cnt,
    int32_t multiget_thread_cnt,
    int32_t merge_thread_cnt);
  virtual void run(CThread *thread, void *arg);
public:
  int64_t get_io_cnt_;
  int64_t multiget_io_cnt_;
  int64_t merge_io_cnt_;
  int64_t err_cnt_;
private:
  void get(const int64_t thread_id);
  void multiget(const int64_t thread_id);
  void merge(const int64_t thread_id);
  int fd_;
  int64_t file_size_;
  int64_t get_thread_cnt_;
  int64_t multiget_thread_cnt_;
  int64_t merge_thread_cnt_;
};

TestIOStress::TestIOStress()
  : get_io_cnt_(0),
    multiget_io_cnt_(0),
    merge_io_cnt_(0),
    err_cnt_(0),
    fd_(),
    file_size_(0),
    get_thread_cnt_(0),
    multiget_thread_cnt_(0),
    merge_thread_cnt_(0)
{
  fd_.disk_id_.disk_idx_ = 0;
  fd_.disk_id_.install_seq_ = 0;
}

TestIOStress::~TestIOStress()
{
}

void TestIOStress::init(
  int fd,
  int64_t file_size,
  int32_t get_thread_cnt,
  int32_t multiget_thread_cnt,
  int32_t merge_thread_cnt)
{
  fd_ = fd;
  file_size_ = file_size;
  get_thread_cnt_ = get_thread_cnt;
  multiget_thread_cnt_ = multiget_thread_cnt;
  merge_thread_cnt_ = merge_thread_cnt;
  set_thread_count(get_thread_cnt + multiget_thread_cnt + merge_thread_cnt);
}

void TestIOStress::get(const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  ObIOHandle io_handle;
  ObIOInfo io_info;
  int64_t i = 0;
  int64_t rd_block_size = 16 * 1024;
  while(!has_set_stop()) {
    io_info.fd_ = fd_;
    io_info.offset_ = (i++ * rd_block_size) % (file_size_ / _threadCount) + (file_size_ / _threadCount) * thread_id;
    io_info.size_ = rd_block_size;
    io_info.io_desc_.category_ = USER_IO;
    io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
    io_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_READ;
    if (OB_SUCCESS != (ret = ObIOManager::get_instance().aio_read(io_info, io_handle))) {
      COMMON_LOG(WARN, "Fail to submit read aio request, ", K(ret));
    } else {
      ATOMIC_INC(&get_io_cnt_);
      if (OB_FAIL(io_handle.wait(DEFAULT_IO_WAIT_TIME_MS))) {
        COMMON_LOG(WARN, "Fail to wait io, ", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      ATOMIC_INC(&err_cnt_);
    }
  }
}

void TestIOStress::multiget(const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  static const int64_t IO_CNT = 1000;
  ObIOInfo io_info;
  ObIOHandle io_handle[IO_CNT];
  int64_t cur = 0;
  int64_t i = 0;
  int64_t rd_block_size = 16 * 1024;
  while(!has_set_stop()) {
    io_info.fd_ = fd_;
    io_info.offset_ = (i++ * rd_block_size) % (file_size_ / _threadCount) + (file_size_ / _threadCount) * thread_id;
    io_info.size_ = rd_block_size;
    io_info.io_desc_.category_ = USER_IO;
    io_info.io_desc_.mode_ = ObIOMode::IO_MODE_READ;
    io_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_DATA_INDEX_READ;
    if (OB_SUCCESS != (ret = ObIOManager::get_instance().aio_read(io_info, io_handle[cur]))) {
      COMMON_LOG(WARN, "Fail to submit read aio request, ", K(ret));
    } else {
      ATOMIC_INC(&multiget_io_cnt_);
    }
    cur = (cur + 1) % IO_CNT;
    if (0 == cur) {
      for (int64_t j = 0; j < IO_CNT; ++j) {
        io_handle[j].wait();
      }
    }

    if (OB_FAIL(ret)) {
      ATOMIC_INC(&err_cnt_);
    }
  }
}

void TestIOStress::merge(const int64_t thread_id)
{
  int ret = OB_SUCCESS;
  ObIOHandle io_handle[2];
  ObIOInfo io_info;
  int64_t cur = 0;
  int64_t i = 0;
  int64_t wr_block_size = 2 * 1024 * 1024;
  char *buffer = (char *) malloc(wr_block_size);
  memset(buffer, 'a', wr_block_size);

  while(!has_set_stop()) {
    if (io_handle[cur].is_valid()) {
      io_handle[cur].wait();
    }
    io_info.fd_ = fd_;
    io_info.offset_ = upper_align((i++ * wr_block_size) % (file_size_ / _threadCount) + (file_size_ / _threadCount) * thread_id
        , 4096);
    io_info.size_ = wr_block_size;
    io_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    if (OB_SUCCESS != (ret = ObIOManager::get_instance().aio_write(io_info, buffer, io_handle[cur]))) {
      COMMON_LOG(WARN, "Fail to submit write aio request, ", K(ret));
    } else {
      ATOMIC_INC(&merge_io_cnt_);
    }
    cur = (cur + 1) % 2;

    if (OB_FAIL(ret)) {
      ATOMIC_INC(&err_cnt_);
    }
  }
  free(buffer);
}

void TestIOStress::run(CThread *thread, void *arg)
{

  int64_t thread_id = (int64_t) arg;

  if (thread_id < get_thread_cnt_) {
    get(thread_id);
  } else if (thread_id < get_thread_cnt_ + multiget_thread_cnt_) {
    multiget(thread_id);
  } else {
    merge(thread_id);
  }
}

TEST_F(TestIOManager, stress)
{
  int ret = OB_SUCCESS;
  TestIOStress io_stress;
  int64_t i = 0;

  int64_t block_size = 2 * 1024 * 1024;
  char buffer[block_size];
  memset(buffer, 'a', block_size);
  ObIOInfo io_info;
  ObIOHandle io_handle;
  io_info.size_ = block_size;

  io_info.batch_count_ = 1;
  ObIOPoint &io_point = io_info.io_points_[0];
  io_point.fd_ = fd_;
  io_point.offset_ = 0;
  io_point.write_buf_ = buffer;
  for (int64_t i = 0; i < file_size_ / block_size; ++i) {
    io_point.offset_ = i * block_size;
    io_handle.reset();
    ret = ObIOManager::get_instance().write(io_info);
    //ret = ObIOManager::get_instance().aio_write(io_info, buffer, io_handle);
    if (OB_FAIL(ret)) {
      --i;
      usleep(100);
    }
    printf("%ld ", i);
  }
  COMMON_LOG(INFO, "Stress prepare completed!");

  io_stress.init(fd_, file_size_, 0, 0, 4);
  io_stress.start();
  int64_t get_io_cnt = 0;
  int64_t last_get_io_cnt = 0;
  int64_t get_iops = 0;
  int64_t multiget_io_cnt = 0;
  int64_t last_multiget_io_cnt = 0;
  int64_t multiget_iops = 0;
  int64_t merge_io_cnt = 0;
  int64_t last_merge_io_cnt = 0;
  int64_t merge_iops = 0;
  while(i++ < 60) {
    sleep(1);
    get_io_cnt = io_stress.get_io_cnt_;
    multiget_io_cnt = io_stress.multiget_io_cnt_;
    merge_io_cnt = io_stress.merge_io_cnt_;
    get_iops = get_io_cnt - last_get_io_cnt;
    multiget_iops = multiget_io_cnt - last_multiget_io_cnt;
    merge_iops = merge_io_cnt - last_merge_io_cnt;
    last_get_io_cnt = get_io_cnt;
    last_multiget_io_cnt = multiget_io_cnt;
    last_merge_io_cnt = merge_io_cnt;
    COMMON_LOG(INFO, "IOPS: ", K(get_iops), K(multiget_iops), K(merge_iops));
  }
  io_stress.stop();
  io_stress.wait();
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
