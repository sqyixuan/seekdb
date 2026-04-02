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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_DIR_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_DIR_MANAGER_H_

#include <stdint.h>
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
namespace storage
{

class ObDirManager
{
public:
  static const int64_t DEFAULT_DIR_SIZE = 4L * 1024L; // 4KB
  static ObDirManager &get_instance();
  int set_object_storage_root_dir(const char *dir_path);
  static int fsync_dir(const char *dir_path);
  // create dir
  static int create_dir(const char *dir_path, const blocksstable::ObStorageObjectType object_type = blocksstable::ObStorageObjectType::MAX);
  static int delete_dir(const char *dir_path, const blocksstable::ObStorageObjectType object_type = blocksstable::ObStorageObjectType::MAX);
  static int delete_dir_rec(const char *path, int64_t &size);
  int create_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int create_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                       const int64_t ls_id, const int64_t ls_epoch_id);
  int create_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t ls_id, const int64_t ls_epoch_id,
                                       const int64_t tablet_id);
  int create_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t tablet_id);
  int create_tablet_data_tablet_id_transfer_seq_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                    const int64_t tablet_id, const int64_t transfer_seq);
  int create_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                          const int64_t tmp_file_id);

  // delete dir
  int delete_tenant_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int delete_ls_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                       const int64_t ls_id, const int64_t ls_epoch_id);
  int delete_tablet_meta_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t ls_id, const int64_t ls_epoch_id,
                                       const int64_t tablet_id);
  int delete_tablet_data_tablet_id_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                       const int64_t tablet_id);
  int delete_tablet_data_tablet_id_transfer_seq_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                                                    const int64_t tablet_id, const int64_t transfer_seq);
  int delete_tmp_file_dir(const uint64_t tenant_id, const int64_t tenant_epoch_id,
                          const int64_t tmp_file_id);

  // get dir path
  int get_cluster_dir(char *path, const int64_t length);
  int get_local_tenant_dir(char *path, const int64_t length,
                           const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_remote_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id,
                            const int64_t tenant_epoch_id);
  int get_shared_tenant_dir(char *path, const int64_t length, const uint64_t tenant_id);
  int get_ls_dir(char *path, const int64_t length,
                 const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_ls_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                    const int64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id);
  int get_local_tablet_data_dir(char *path, const int64_t length,
                                const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_remote_tablet_data_dir(char *path, const int64_t length,
                                 const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_tablet_data_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                    const int64_t tenant_epoch_id, const int64_t tablet_id);
  int get_tablet_data_tablet_id_transfer_seq_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                                 const int64_t tenant_epoch_id, const int64_t tablet_id, const int64_t transfer_seq);
  int get_local_tablet_id_macro_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                    const int64_t tenant_epoch_id, const int64_t tablet_id,
                                    const int64_t transfer_seq, const ObMacroType macro_type);
  int get_remote_tablet_id_macro_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                     const int64_t tenant_epoch_id, const int64_t tablet_id,
                                     const int64_t transfer_seq, const ObMacroType macro_type);
  int get_tablet_meta_dir(char *path, const int64_t length, const uint64_t tenant_id,
                          const int64_t tenant_epoch_id, const int64_t ls_id, const int64_t ls_epoch_id);
  int get_tablet_meta_tablet_id_dir(char *path, const int64_t length, const uint64_t tenant_id,
                                    const int64_t tenant_epoch_id, const int64_t ls_id,
                                    const int64_t ls_epoch_id, const int64_t tablet_id);
  int get_shared_tablet_meta_dir(char *path, const int64_t length, const int64_t tablet_id);
  int get_shared_tablet_data_dir(char *path, const int64_t length, const int64_t tablet_id);
  int get_shared_tablet_ids_dir(char *path, const int64_t length);
  int get_shared_tablet_dir(char *path, const int64_t length, const int64_t tablet_id);
  int get_local_tmp_data_dir(char *path, const int64_t length,
                             const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_remote_tmp_data_dir(char *path, const int64_t length,
                              const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_local_tmp_file_dir(char *path, const int64_t length, const uint64_t tenant_id,
                             const int64_t tenant_epoch_id, const int64_t tmp_file_id);
  int get_remote_tmp_file_dir(char *path, const int64_t length, const uint64_t tenant_id,
                              const int64_t tenant_epoch_id, const int64_t tmp_file_id);
  int get_local_major_data_dir(char *path, const int64_t length,
                               const uint64_t tenant_id, const int64_t tenant_epoch_id);
  int get_object_storage_root_dir(char *&path);
  OB_INLINE const char *get_local_cache_root_dir() const { return OB_FILE_SYSTEM_ROUTER.get_sstable_dir(); }
private:
  ObDirManager();
  virtual ~ObDirManager();

private:
  char object_storage_root_dir_[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH];
};

#define OB_DIR_MGR (oceanbase::storage::ObDirManager::get_instance())

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_DIR_MANAGER_H_ */
