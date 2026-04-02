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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FORMAT_UTIL_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FORMAT_UTIL_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_ss_common_header.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObSSFormatBody final
{
public:
  static const int32_t SS_FORMAT_VERSION = 1;
  static const int64_t META_BUFF_SIZE = 4096;
  const char *const VERSION_ID_STR = "version_id=";
  const char *const CLUSTER_VERSION_STR = "cluster_version=";
  const char *const CREATE_TIMESTAMP_STR = "create_timestamp=";
  ObSSFormatBody();
  ~ObSSFormatBody() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(version),
               K_(cluster_version),
               K_(create_timestamp));
  OB_UNIS_VERSION(SS_FORMAT_VERSION);
public:
  int64_t version_;
  uint64_t cluster_version_;
  int64_t create_timestamp_;
};

struct ObSSFormat final
{
public:
  ObSSFormat();
  ~ObSSFormat() = default;
  ObSSFormat &operator==(const ObSSFormat &other) = delete;
  ObSSFormat &operator!=(const ObSSFormat &other) = delete;
  void reset();
  int init(const uint64_t cluster_version, int64_t create_timestamp);
  bool is_valid() const;
  TO_STRING_KV(K_(header), K_(body));
  OB_UNIS_VERSION(1);
private:
  ObSSCommonHeader header_;
  ObSSFormatBody body_;
};

class ObSSFormatUtil
{
public:
  static int write_ss_format(const share::ObBackupDest &storage_dest,
                             const ObSSFormat &ss_format);
  static int read_ss_format(const share::ObBackupDest &storage_dest,
                            ObSSFormat &ss_format);
  static int is_exist_ss_format(const share::ObBackupDest &storage_dest,
                                bool &is_exist);
  static int get_ss_format_path(const share::ObBackupDest &storage_dest,
                                char *path, const int64_t length);
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_FORMAT_UTIL_H_ */
