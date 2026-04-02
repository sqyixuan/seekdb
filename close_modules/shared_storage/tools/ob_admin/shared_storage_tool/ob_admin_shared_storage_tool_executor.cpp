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
#include "ob_admin_shared_storage_tool_executor.h"

#include "src/share/ob_device_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
#define HELP_FMT "\t%-30s%-12s\n"

namespace oceanbase
{
namespace tools
{
ObAdminSSToolExecutor::ObAdminSSToolExecutor()
    : ObAdminExecutor(), 
      cmd_(ObAdminSSToolCmd::MAX), 
      cmd_ctx_(),
      is_quiet_(false),
      io_allocator_()
{}

int ObAdminSSToolExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  if (OB_SUCC(parse_cmd(argc, argv))) {
    OB_LOGGER.set_log_level(is_quiet_ ? "ERROR" : "INFO");

    if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
      STORAGE_LOG(WARN, "init device manager failed", KR(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().init())) {
      STORAGE_LOG(WARN, "failed to init io manager", K(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().start())) {
      STORAGE_LOG(WARN, "failed to start io manager", K(ret));
    } else if (OB_FAIL(prepare_decoder())) {
      STORAGE_LOG(WARN, "fail to prepare_decoder", K(ret));
    } else {
      STORAGE_LOG(INFO, "cmd is", K(cmd_));
      switch (cmd_) {
        case ObAdminSSToolCmd::DUMP_SS_PHY_BLOCK:
          dump_ss_phy_block(cmd_ctx_.file_path_, cmd_ctx_.tenant_id_,
              cmd_ctx_.block_idx_, cmd_ctx_.only_index_, cmd_ctx_.is_micro_meta_);
          break;
        case ObAdminSSToolCmd::DOWNLOAD_SS_MACRO_BLOCK:
          download_ss_macro_block(cmd_ctx_.uri_, cmd_ctx_.storage_info_str_);
          break;
        case ObAdminSSToolCmd::CAL_CRC:
          cal_crc(cmd_ctx_.file_path_, cmd_ctx_.offset_, cmd_ctx_.size_);
          break;
        default:
          print_usage();
          exit(1);
      }
    }
  }
  return ret;
}

int ObAdminSSToolExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char *opt_str = "hc:u:k:f:o:t:n:a:iqv:";
  struct option longopts[] = {
      // commands
      {"help", 0, NULL, 'h'},
      {"command", 1, NULL, 'c'},
      // options
      {"uri", 1, NULL, 'u'},
      {"storage_info", 1, NULL, 'k'},
      {"file_path", 1, NULL, 'f'},
      {"offset", 1, NULL, 'o'},
      {"size", 1, NULL, 't'},
      {"block_idx", 1, NULL, 'n'},
      {"tenant_id", 1, NULL, 'a'},
      {"only_micro_index", 0, NULL, 'i'},
      {"quiet", 0, NULL, 'q'},
      {"ckpt_type", 1, NULL, 'v'},
      {NULL, 0, NULL, 0}};

  int index = -1;
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        print_usage();
        exit(1);
      }
      case 'c': {
        if (0 == strcmp(optarg, "dump_phy_block")) {
          cmd_ = ObAdminSSToolCmd::DUMP_SS_PHY_BLOCK;
        } else if (0 == strcmp(optarg, "dl") || 0 == strcmp(optarg, "download_ss_macro_block")) {
          cmd_ = ObAdminSSToolCmd::DOWNLOAD_SS_MACRO_BLOCK;
        } else if (0 == strcmp(optarg, "crc")) {
          cmd_ = ObAdminSSToolCmd::CAL_CRC;
        } else {
          print_usage();
          exit(1);
        }
        break;
      }
      case 'v': {
        if (0 == strcmp(optarg, "phy_block")) {
          cmd_ctx_.is_micro_meta_ = false;
        } else if (0 == strcmp(optarg, "micro_meta")) {
          cmd_ctx_.is_micro_meta_ = true;
        } else {
          print_usage();
          exit(1);
        }
        break;
      }
      case 'u': {
        time_t timestamp = time(NULL);
        struct tm *timeinfo = localtime(&timestamp);
        char buffer[OB_MAX_TIME_STR_LENGTH];
        strftime(buffer, sizeof(buffer), "%Y-%m-%d-%H:%M:%S", timeinfo);
        if (OB_FAIL(databuff_printf(cmd_ctx_.uri_, sizeof(cmd_ctx_.uri_), "%s", optarg))) {
          OB_LOG(WARN, "failed to construct base path", K(ret), K((char *)optarg), K(buffer));
        }
        break;
      }
      case 'k': {
        if (OB_FAIL(databuff_printf(cmd_ctx_.storage_info_str_, sizeof(cmd_ctx_.storage_info_str_), "%s", optarg))) {
          OB_LOG(WARN, "failed to copy storage info str", K(ret));
        }
        break;
      }
      case 'f': {
        if (OB_FAIL(databuff_printf(cmd_ctx_.file_path_, sizeof(cmd_ctx_.file_path_), "%s", optarg))) {
          OB_LOG(WARN, "failed to copy file path", K(ret));
        }
        break;
      }
      case 'n': {
        cmd_ctx_.block_idx_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'a': {
        cmd_ctx_.tenant_id_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'i': {
        cmd_ctx_.only_index_ = true;
        break;
      }
      case 'o': {
        cmd_ctx_.offset_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 't': {
        cmd_ctx_.size_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'q': {
        is_quiet_ = true;
        break;
      }
      default: {
        print_usage();
        exit(1);
      }
    }
  }

  return ret;
}

int ObAdminSSToolExecutor::download_ss_macro_block(
    const char *uri, 
    const char *storage_info_str)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  ObBackupStorageInfo storage_info;
  char *macro_buf = nullptr;
  int64_t buf_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  int64_t file_len = 0;
  ObIOFd fd;
  fd.second_id_ = fileno(stdout);
  io_allocator_.reuse();
  STORAGE_LOG(INFO, "begin download_ss_macro_block");
  if (STRLEN(uri) == 0 || STRLEN(storage_info_str) == 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), KP(uri), KP(storage_info_str));
  } else if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc read buffer", KR(ret), K(buf_size));
  } else if (OB_FAIL(storage_info.set(uri, storage_info_str))) {
    STORAGE_LOG(WARN, "fail to set storage info", KR(ret), K(uri));
  } else if (OB_FAIL(adapter.get_file_length(uri, &storage_info, file_len))) {
    STORAGE_LOG(WARN, "fail to get file len", KR(ret), K(uri));
  } else {
    int64_t cur_offset = 0;
    while (OB_SUCC(ret) && (cur_offset < file_len)) {
      int64_t read_size = 0;
      int64_t write_size = 0;
      buf_size = MIN(OB_DEFAULT_MACRO_BLOCK_SIZE, file_len - cur_offset);
      if (OB_FAIL(adapter.read_part_file(uri, &storage_info, macro_buf, buf_size, cur_offset, read_size, common::ObStorageIdMod()))) {
        STORAGE_LOG(WARN, "fail to read part file", KR(ret), K(uri), K(buf_size), K(cur_offset));
      } else if (OB_UNLIKELY(buf_size != read_size)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "read size wrong", KR(ret), K(read_size), K(buf_size), K(cur_offset));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, macro_buf, read_size, write_size))) {
        STORAGE_LOG(WARN, "fail to write", KR(ret), K(fd), KP(macro_buf), K(read_size), K(write_size));
      } else if (OB_UNLIKELY(write_size != read_size)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "write_size is wrong", KR(ret), K(write_size), K(read_size));
      } else {
        cur_offset += read_size;
      }
    }
  }
  io_allocator_.clear();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to download_ss_macro_block, ret=%s\n", ob_error_name(ret));
  }
  return ret;
}

int ObAdminSSToolExecutor::dump_ss_phy_block(
    const char *file, 
    const uint64_t tenant_id, 
    const int64_t block_idx, 
    const bool only_index,
    const bool is_micro_meta)
{
  int ret = OB_SUCCESS;
  io_allocator_.reuse();
  char *data_buf = nullptr;
  int64_t block_size = OB_DEFAULT_MACRO_BLOCK_SIZE;

  STORAGE_LOG(INFO, "begin dump ss_phy_block");
  if (OB_UNLIKELY(block_idx < 0 || !is_valid_tenant_id(tenant_id) || STRLEN(file) == 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(block_idx), K(tenant_id), K(file));
  } else if (OB_ISNULL(data_buf = reinterpret_cast<char *>(io_allocator_.alloc(block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc data_buf for reading phy_blk", KR(ret), K(block_size));
  } else {
    ObIOFd fd;
    int64_t read_size = 0;
    int64_t block_offset = block_size * block_idx;
    if (OB_FAIL(ObIODeviceLocalFileOp::open(file, O_RDONLY, 0, fd))) {
      STORAGE_LOG(WARN, "fail to open micro_cache_file", KR(ret), K(file));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(fd.second_id_, data_buf, block_size, block_offset, read_size))) {
      STORAGE_LOG(WARN, "fail to read ss_phy_block", KR(ret), K(fd), K(block_size), K(block_offset), K(read_size));
    } else if (OB_UNLIKELY(read_size != block_size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read_size is wrong", KR(ret), K(block_size), K(read_size));
    } else {
      const char *dir = "/tmp/dump_phy_block/";
      ObIODeviceLocalFileOp::mkdir(dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);

      int p_ret = 0;
      char full_path[common::MAX_PATH_SIZE] = {0};
      if (only_index) {
        p_ret = snprintf(full_path, sizeof(full_path), "%s/%ld_%ld_index", dir, tenant_id, block_idx);
      } else {
        p_ret = snprintf(full_path, sizeof(full_path), "%s/%ld_%ld", dir, tenant_id, block_idx);
      }
      if (p_ret < 0 || p_ret >= sizeof(full_path)) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(WARN, "file name too long", KR(ret), K(dir), K(block_idx));
      } else if (OB_FAIL(ObSSMicroCacheUtil::dump_phy_block(data_buf, block_size, full_path, only_index, is_micro_meta))) {
        STORAGE_LOG(WARN, "fail to dump ss_phy_block", KR(ret), K(block_size), K(full_path), K(tenant_id));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (fd.is_valid() && OB_TMP_FAIL(ObIODeviceLocalFileOp::close(fd))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      STORAGE_LOG(WARN, "fail to close micro_cache_file", KR(ret), KR(tmp_ret), K(fd));
    }
  }
  io_allocator_.clear();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_ss_phy_block, ret=%s\n", ob_error_name(ret));
  }
  return ret;
}

int ObAdminSSToolExecutor::cal_crc(
    const char *file, 
    const int64_t offset, 
    const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(STRLEN(file) == 0 || offset < 0 || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(file), K(offset), K(size));
  } else {
    ObIOFd fd;
    char *data_buf = nullptr;
    int64_t read_size = 0;
    if (OB_ISNULL(data_buf = reinterpret_cast<char *>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc data_buf for reading file", KR(ret), K(size));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::open(file, O_RDONLY, 0, fd))) {
      STORAGE_LOG(WARN, "fail to open file", KR(ret), K(file), K(fd));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::pread_impl(fd.second_id_, data_buf, size, offset, read_size))) {
      STORAGE_LOG(WARN, "fail to read file", KR(ret), K(fd), KP(data_buf), K(size), K(offset), K(read_size));
    } else if (OB_UNLIKELY(read_size != size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "read size is wrong", KR(ret), K(read_size), K(size));
    } else {
      uint64_t crc64 = ob_crc64(data_buf, size);
      fprintf(stdout, "crc64: %ld", crc64);
    }

    int tmp_ret = OB_SUCCESS;
    if (fd.is_valid() && OB_TMP_FAIL(ObIODeviceLocalFileOp::close(fd))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      STORAGE_LOG(WARN, "fail to close file", KR(ret), KR(tmp_ret), K(fd));
    }
  }

  io_allocator_.clear();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to cal_crc, ret=%s", ob_error_name(ret));
  }
  return ret;
}

void ObAdminSSToolExecutor::print_usage()
{
  printf("\n");
  printf("Usage: shared_storage tool command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");
  printf(HELP_FMT, "-c,--command", "command, args: [dump_phy_block, download_ss_macro_block, crc]");

  printf("options:\n");
  printf(HELP_FMT, "-u,--uri", "the uri of object storage");
  printf(HELP_FMT, "-k,--storage_info", "should provide storage info");
  printf(HELP_FMT, "-f,--file_path", "micro cache file path");
  printf(HELP_FMT, "-o,--offset", "file offset");
  printf(HELP_FMT, "-t,--size", "object size");
  printf(HELP_FMT, "-n,--block_idx", "block idx");
  printf(HELP_FMT, "-a,--tenant_id", "tenant id");
  printf(HELP_FMT, "-i,--only_micro_index", "only micro_index, used by dump normal_blk");
  printf(HELP_FMT, "-q,--quiet", "log level: ERROR");
  printf(HELP_FMT, "-v,--ckpt_type", "checkpoint type, used by dump ckpt_blk");

  printf("samples:\n");
  printf("  download ss macro block: \n");
  printf("\tob_admin ss_tool -c download_ss_macro_block -u URI -k AK&&SK\n");
  printf("  dump micro cache physical block, including super_blk, normal_blk, ckpt_blk: \n");
  printf("\tob_admin ss_tool -c dump_phy_block -f micro_cache_file -a tenant_id -n super_blk_idx\n");
  printf("\tob_admin ss_tool -c dump_phy_block -f micro_cache_file -a tenant_id -n normal_blk_idx -i\n");
  printf("\tob_admin ss_tool -c dump_phy_block -f micro_cache_file -a tenant_id -n ckpt_blk_idx -v [micro_meta, phy_block]\n");
  printf("  calculate crc64: \n");
  printf("\tob_admin ss_tool -c crc -f file_path -o offset -t size\n");
}
}   //tools 
}   //oceanbase
