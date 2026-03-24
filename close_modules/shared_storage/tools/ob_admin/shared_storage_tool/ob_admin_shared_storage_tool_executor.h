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

#ifndef OB_ADMIN_SHARED_STORAGE_TOOL_EXECUTOR_H_
#define OB_ADMIN_SHARED_STORAGE_TOOL_EXECUTOR_H_
#include "tools/ob_admin/ob_admin_executor.h"

namespace oceanbase
{
namespace tools
{
enum class ObAdminSSToolCmd
{
  DOWNLOAD_SS_MACRO_BLOCK,
  DUMP_SS_PHY_BLOCK,
  CAL_CRC,
  DUMP_SSI_META,
  MAX,
};

struct ObAdminSSToolCmdContext final
{
public:
  ObAdminSSToolCmdContext()
      : tenant_id_(OB_INVALID_TENANT_ID), block_idx_(0), offset_(0), size_(0), only_index_(false), is_micro_meta_(false)
  {}
  ~ObAdminSSToolCmdContext() = default;
  TO_STRING_KV(
      K_(tenant_id), K_(block_idx), K_(offset), K_(size),K_(only_index), K_(is_micro_meta), K_(file_path), K_(uri));

  uint64_t tenant_id_;
  int64_t block_idx_;
  int64_t offset_;
  int64_t size_;
  bool only_index_;
  bool is_micro_meta_;
  char file_path_[common::OB_MAX_FILE_NAME_LENGTH] = {0};
  char uri_[common::OB_MAX_URI_LENGTH] = {0};
  char storage_info_str_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
};

class ObAdminSSToolExecutor : public ObAdminExecutor
{
public:
  ObAdminSSToolExecutor();
  virtual ~ObAdminSSToolExecutor() {}
  virtual int execute(int argc, char *argv[]) override;

private:
  int parse_cmd(int argc, char *argv[]);
  int download_ss_macro_block(const char *uri, const char *storage_info_str);
  int dump_ss_phy_block(const char *file, const uint64_t tenant_id, const int64_t block_idx,
      const bool only_index, const bool is_micro_meta);
  int cal_crc(const char *file, const int64_t offset, const int64_t size);
  void print_usage();

private:
  ObAdminSSToolCmd cmd_;
  ObAdminSSToolCmdContext cmd_ctx_;
  bool is_quiet_;
  common::ObArenaAllocator io_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObAdminSSToolExecutor);
};

} //namespace tools
} //namespace oceanbase
#endif  // OB_ADMIN_SHARED_STORAGE_TOOL_EXECUTOR_H_
