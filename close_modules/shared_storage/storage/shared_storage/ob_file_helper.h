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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_HELPER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_HELPER_H_

#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

const char *const CLUSTER_DIR_STR = "cluster";
const char *const SERVER_DIR_STR = "server";
const char *const TENANT_DIR_STR = "tenant";
const char *const LS_DIR_STR = "ls";
const char *const TABLET_DIR_STR = "tablet";
const char *const TABLET_IDS_DIR_STR = "tablet_ids";
const char *const TABLET_DATA_DIR_STR = "tablet_data";
const char *const TABLET_META_DIR_STR = "tablet_meta";
const char *const DEFAULT_TMP_STR = ".tmp.";
const char *const DEFAULT_DELETED_STR = ".deleted";
const char *const MINI_DIR_STR = "mini";
const char *const MINOR_DIR_STR = "minor";
const char *const MAJOR_DIR_STR = "major";
const char *const COLUMN_GROUP_STR = "cg";
const char *const DATA_MACRO_DIR_STR = "data";
const char *const META_MACRO_DIR_STR = "meta";
const char *const TMP_DATA_DIR_STR = "tmp_data";
const char *const MAJOR_DATA_DIR_STR = "shared_major_macro_cache";
const char *const COMPACTION_DIR_STR = "compaction";
const char *const COMPACTOR_DIR_STR = "compactor";
const char *const SCHEDULER_DIR_STR = "scheduler";
const char *const SHARED_TABLET_META_DIR_STR = "meta";
const char *const SHARED_TABLET_SSTABLE_DIR_STR = "sstable";
const char *const MICRO_CACHE_FILE_NAME = "micro_cache_file";
const char *const SS_FORMAT_FILE_NAME = "ss_format";
const char *const CKM_ERROR_DIR_STR = "checksum_error_macro";

enum ObMacroType
{
  DATA_MACRO = 0,
  META_MACRO = 1,
};

static const char *macro_type_strs[] = {DATA_MACRO_DIR_STR, META_MACRO_DIR_STR};

static const char *get_macro_type_str(const ObMacroType type)
{
  return macro_type_strs[static_cast<int32_t>(type)];
}

class ObPathContext
{
public:
  ObPathContext();
  ~ObPathContext();
  ObPathContext& operator =(const ObPathContext &other);
  bool is_valid() const;
  void reset();
  int set_file_ctx(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, const bool is_local_cache);
  int set_logical_delete_ctx(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id);  // set for .deleted file
  int set_atomic_write_ctx(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id, const uint64_t write_seq);  // set for .tmp.seq file
  OB_INLINE const char *get_path() const { return path_; }
  TO_STRING_KV(K_(path), K_(file_id), K_(ls_epoch_id), K_(is_local_cache), K_(is_atomic_write), K_(write_seq), K_(is_logical_delete));
private:
  bool is_macro_block_id_valid() const;
  int to_path();
private:
  char path_[common::MAX_PATH_SIZE];
  blocksstable::MacroBlockId file_id_;
  int64_t ls_epoch_id_;  // meta_file need ls_epoch_id, because meta_file is stored in ls_dir
  bool is_local_cache_;  // store in local is true, store in remote is false
  bool is_atomic_write_;  // if true, concat .tmp.seq to path
  uint64_t write_seq_;  // used for atomic_write
  bool is_logical_delete_;  // if true, concat .deleted to path
};

class ObFileHelper
{
public:
  static int parse_file_name(const char *file_name, int64_t &file_id); // only used for file_name is a number
  static int parse_tmp_file_name(const char *file_name, int64_t &file_id); // only used for tmp_file_name is a number or number.deleted
  static int tmpfile_path_to_macro_id(const char *dir_path, const char *file_name, blocksstable::MacroBlockId &file_id); // convert tmpfile path to macro id
  static int get_file_parent_dir(char *path, const int64_t length,
                                 const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id = 0);
  static int create_file_parent_dir(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id);

};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_HELPER_H_ */
